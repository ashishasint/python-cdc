import logging
import json
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from queue import Queue

import threading
import time

from utils.cdc_config import CDCConfig
from utils.postgre_cdc_consumer import PostgresCDCConsumer


EVENT_QUEUE = Queue(maxsize=1000)  # backpressure protection

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass  # Suppress health check logs


def start_health_server(port: int):
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    logger.info(f"Health check server started on port {port}")
    server.serve_forever()


def worker(worker_id: int, queue: Queue):
    print(f"⚙️ Worker-{worker_id} started")

    while True:
        event = queue.get()  # blocks until event available

        try:
            print(
                f"⚙️ Worker-{worker_id} processing "
                f"{event.operation} on {event.schema}.{event.table}"
            )

            # 🔥 simulate real work
            time.sleep(1)

            print("\n" + "=" * 60)
            print(f"🔔 CDC EVENT: {event.operation}")
            print(f"   Table: {event.schema}.{event.table}")
            print(f"   Time: {event.timestamp}")

            if event.old_values:
                print(f"   Old Values: {json.dumps(event.old_values, indent=6)}")
            if event.new_values:
                print(f"   New Values: {json.dumps(event.new_values, indent=6)}")

            print("=" * 60)

        except Exception as e:
            print(f"❌ Worker-{worker_id} failed: {e}")

        finally:
            queue.task_done()


def main():
    # Start health check server for Cloud Run
    health_port = int(os.environ.get("PORT", 8080))
    health_thread = threading.Thread(
        target=start_health_server, args=(health_port,), daemon=True
    )
    health_thread.start()

    config = CDCConfig()

    consumer = PostgresCDCConsumer(config)

    def handle_event(event):
        """
        Producer: puts CDC events into queue
        """
        EVENT_QUEUE.put(event, block=True)
        print(f"📥 Enqueued: {event.operation} {event.table}")

    try:
        consumer.connect()
        consumer.create_replication_slot()

        # Start worker threads

        for i in range(int(os.environ.get("CDC_WORKER_COUNT", 3))):
            t = threading.Thread(target=worker, args=(i, EVENT_QUEUE), daemon=True)
            t.start()

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
