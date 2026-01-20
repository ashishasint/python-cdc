import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from psycopg2 import sql
import struct
from dataclasses import dataclass, field
from typing import Callable, Optional, Dict, Any, List
from datetime import datetime
import logging
import json
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class CDCConfig:
    host: str = "localhost"
    port: int = 5433
    user: str = "postgres"
    password: str = "Vasv@9344"
    database: str = "cdc_demo"
    slot_name: str = "python_cdc_slot"  # Unique name for this consumer
    publication_name: str = "cdc_publication"  # Which tables to watch
    offset_file: str = "offsets.json"


@dataclass
class CDCEvent:
    operation: str  # INSERT, UPDATE, DELETE
    schema: str
    table: str
    columns: List[str]
    old_values: Optional[Dict[str, Any]] = None  # For UPDATE/DELETE
    new_values: Optional[Dict[str, Any]] = None  # For INSERT/UPDATE
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return {
            "operation": self.operation,
            "schema": self.schema,
            "table": self.table,
            "columns": self.columns,
            "old_values": self.old_values,
            "new_values": self.new_values,
            "timestamp": self.timestamp,
        }


class PgOutputParser:
    """Parser for PostgreSQL pgoutput logical replication protocol"""

    def __init__(self):
        self.relations: Dict[int, dict] = {}  # relation_id -> relation info

    def parse_message(self, payload: bytes) -> Optional[CDCEvent]:
        """Parse a pgoutput protocol message"""
        if not payload:
            return None

        msg_type = chr(payload[0])
        data = payload[1:]

        if msg_type == "R":  # Relation message
            self._parse_relation(data)
            return None
        elif msg_type == "I":  # Insert
            return self._parse_insert(data)
        elif msg_type == "U":  # Update
            return self._parse_update(data)
        elif msg_type == "D":  # Delete
            return self._parse_delete(data)
        elif msg_type == "B":  # Begin
            logger.debug("Transaction BEGIN")
            return None
        elif msg_type == "C":  # Commit
            logger.debug("Transaction COMMIT")
            return None

        return None

    def _parse_relation(self, data: bytes):
        """Parse relation (table) metadata message"""
        pos = 0

        # Relation ID (4 bytes)
        relation_id = struct.unpack(">I", data[pos : pos + 4])[0]
        pos += 4

        # Namespace (schema) - null terminated string
        namespace_end = data.index(b"\x00", pos)
        namespace = data[pos:namespace_end].decode("utf-8")
        pos = namespace_end + 1

        # Relation name - null terminated string
        name_end = data.index(b"\x00", pos)
        relation_name = data[pos:name_end].decode("utf-8")
        pos = name_end + 1

        # Replica identity (1 byte)
        replica_identity = chr(data[pos])
        pos += 1

        # Number of columns (2 bytes)
        num_columns = struct.unpack(">H", data[pos : pos + 2])[0]
        pos += 2

        columns = []
        for _ in range(num_columns):
            # Flags (1 byte)
            flags = data[pos]
            pos += 1

            # Column name - null terminated
            col_name_end = data.index(b"\x00", pos)
            col_name = data[pos:col_name_end].decode("utf-8")
            pos = col_name_end + 1

            # Data type ID (4 bytes)
            type_id = struct.unpack(">I", data[pos : pos + 4])[0]
            pos += 4

            # Type modifier (4 bytes)
            type_modifier = struct.unpack(">i", data[pos : pos + 4])[0]
            pos += 4

            columns.append({"name": col_name, "type_id": type_id, "flags": flags})

        self.relations[relation_id] = {
            "schema": namespace,
            "table": relation_name,
            "columns": columns,
            "replica_identity": replica_identity,
        }

        logger.info(
            f"Registered relation: {namespace}.{relation_name} with {num_columns} columns"
        )

    def _parse_tuple_data(self, data: bytes, pos: int, columns: list) -> tuple:
        """Parse tuple data and return (values_dict, new_position)"""
        # Number of columns (2 bytes)
        num_cols = struct.unpack(">H", data[pos : pos + 2])[0]
        pos += 2

        values = {}
        for i in range(num_cols):
            col_type = chr(data[pos])
            pos += 1

            if col_type == "n":  # NULL
                values[columns[i]["name"]] = None
            elif col_type == "u":  # Unchanged (for updates)
                values[columns[i]["name"]] = "[unchanged]"
            elif col_type == "t":  # Text
                # Length (4 bytes)
                length = struct.unpack(">I", data[pos : pos + 4])[0]
                pos += 4
                # Value
                value = data[pos : pos + length].decode("utf-8")
                pos += length
                values[columns[i]["name"]] = value
            elif col_type == "b":  # Binary
                length = struct.unpack(">I", data[pos : pos + 4])[0]
                pos += 4
                values[columns[i]["name"]] = data[pos : pos + length].hex()
                pos += length

        return values, pos

    def _parse_insert(self, data: bytes) -> Optional[CDCEvent]:
        """Parse INSERT message"""
        pos = 0

        # Relation ID (4 bytes)
        relation_id = struct.unpack(">I", data[pos : pos + 4])[0]
        pos += 4

        relation = self.relations.get(relation_id)
        if not relation:
            logger.warning(f"Unknown relation ID: {relation_id}")
            return None

        # 'N' for new tuple
        if chr(data[pos]) != "N":
            return None
        pos += 1

        new_values, _ = self._parse_tuple_data(data, pos, relation["columns"])

        return CDCEvent(
            operation="INSERT",
            schema=relation["schema"],
            table=relation["table"],
            columns=[c["name"] for c in relation["columns"]],
            new_values=new_values,
        )

    def _parse_update(self, data: bytes) -> Optional[CDCEvent]:
        """Parse UPDATE message"""
        pos = 0

        # Relation ID (4 bytes)
        relation_id = struct.unpack(">I", data[pos : pos + 4])[0]
        pos += 4

        relation = self.relations.get(relation_id)
        if not relation:
            return None

        old_values = None
        new_values = None

        # Check for old tuple ('O' or 'K')
        tuple_type = chr(data[pos])
        if tuple_type in ("O", "K"):
            pos += 1
            old_values, pos = self._parse_tuple_data(data, pos, relation["columns"])
            tuple_type = chr(data[pos])

        # New tuple ('N')
        if tuple_type == "N":
            pos += 1
            new_values, _ = self._parse_tuple_data(data, pos, relation["columns"])

        return CDCEvent(
            operation="UPDATE",
            schema=relation["schema"],
            table=relation["table"],
            columns=[c["name"] for c in relation["columns"]],
            old_values=old_values,
            new_values=new_values,
        )

    def _parse_delete(self, data: bytes) -> Optional[CDCEvent]:
        """Parse DELETE message"""
        pos = 0

        # Relation ID (4 bytes)
        relation_id = struct.unpack(">I", data[pos : pos + 4])[0]
        pos += 4

        relation = self.relations.get(relation_id)
        if not relation:
            return None

        # Old tuple type ('O' or 'K')
        tuple_type = chr(data[pos])
        pos += 1

        old_values, _ = self._parse_tuple_data(data, pos, relation["columns"])

        return CDCEvent(
            operation="DELETE",
            schema=relation["schema"],
            table=relation["table"],
            columns=[c["name"] for c in relation["columns"]],
            old_values=old_values,
        )


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
            self.cursor.consume_stream(consume_message)
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


def main():
    config = CDCConfig(
        host="localhost",
        port=5433,
        user="postgres",
        password="Vasv@9344",
        database="cdc_demo",
        slot_name="python_cdc_slot",
        publication_name="cdc_publication",
    )

    consumer = PostgresCDCConsumer(config)

    def handle_event(event: CDCEvent):
        """Callback to handle CDC events"""
        print("\n" + "=" * 60)
        print(f"🔔 CDC EVENT: {event.operation}")
        print(f"   Table: {event.schema}.{event.table}")
        print(f"   Time: {event.timestamp}")

        if event.old_values:
            print(f"   Old Values: {json.dumps(event.old_values, indent=6)}")
        if event.new_values:
            print(f"   New Values: {json.dumps(event.new_values, indent=6)}")

        print("=" * 60)

    try:
        consumer.connect()
        consumer.create_replication_slot()
        consumer.start_replication(handle_event)
    except KeyboardInterrupt:
        logger.info("\nShutting down gracefully...")
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
