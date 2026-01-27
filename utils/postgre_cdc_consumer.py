import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from typing import Callable, Optional
import logging

from dotenv import load_dotenv
from queue import Queue


from utils.cdc_config import CDCConfig
from utils.cdc_event import CDCEvent
from utils.pg_output_parser import PgOutputParser

EVENT_QUEUE = Queue(maxsize=1000)  # backpressure protection

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PostgresCDCConsumer:
    def __init__(self, config: CDCConfig):
        self.config = config
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor = None
        self.parser = PgOutputParser()
        self.running = False

    def connect(self):
        """Establish a logical replication connection"""
        self.connection = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            connection_factory=LogicalReplicationConnection,
        )
        self.cursor = self.connection.cursor()
        logger.info(
            f"Connected to PostgreSQL at {self.config.host}:{self.config.port}/{self.config.database}"
        )

    def create_replication_slot(self):
        """Create replication slot if it doesn't exist"""
        try:
            self.cursor.create_replication_slot(
                slot_name=self.config.slot_name, output_plugin="pgoutput"
            )
            logger.info(f"Created replication slot: {self.config.slot_name}")
        except psycopg2.errors.DuplicateObject:
            logger.info(f"Replication slot '{self.config.slot_name}' already exists")
        except Exception as e:
            logger.error(f"Error creating slot: {e}")
            raise

    def drop_replication_slot(self):
        """Drop the replication slot"""
        try:
            self.cursor.drop_replication_slot(self.config.slot_name)
            logger.info(f"Dropped replication slot: {self.config.slot_name}")
        except Exception as e:
            logger.error(f"Error dropping slot: {e}")

    def start_replication(self, callback: Callable[[CDCEvent], None]):
        """Start consuming CDC events"""
        self.running = True

        # Start replication with pgoutput plugin
        self.cursor.start_replication(
            slot_name=self.config.slot_name,
            decode=False,  # Binary protocol
            options={
                "proto_version": "1",
                "publication_names": self.config.publication_name,
            },
        )

        logger.info("=" * 50)
        logger.info("CDC Consumer started! Listening for changes...")
        logger.info("=" * 50)

        def consume_message(msg):
            """Process each replication message"""
            if not self.running:
                raise StopIteration

            try:
                payload = msg.payload
                event = self.parser.parse_message(payload)

                if event:
                    callback(event)

                # Send feedback to PostgreSQL (acknowledges the message)
                msg.cursor.send_feedback(flush_lsn=msg.data_start)

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                import traceback

                traceback.print_exc()

        try:
            self.cursor.consume_stream(consume_message, keepalive_interval=10)
        except StopIteration:
            logger.info("Replication stream stopped")

    def stop(self):
        """Stop the replication"""
        self.running = False

    def close(self):
        """Close the connection"""
        self.running = False
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Connection closed")
