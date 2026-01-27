from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class CDCConfig:
    host: str = os.environ.get("PG_HOST", "localhost")
    port: int = int(os.environ.get("PG_PORT", 5433))
    user: str = os.environ.get("PG_USER", "postgres")
    password: str = os.environ.get("PG_PASSWORD", "Vasv@9344")
    database: str = os.environ.get("PG_DATABASE", "cdc_demo")
    slot_name: str = os.environ.get("PG_SLOT_NAME", "python_cdc_slot")
    publication_name: str = os.environ.get("PG_PUBLICATION", "cdc_publication")
    offset_file: str = "offsets.json"
