from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DBConfig:
    host: str = os.environ.get("PG_HOST", "localhost")
    port: int = int(os.environ.get("PG_PORT", 5433))
    user: str = os.environ.get("PG_USER", "postgres")
    password: str = os.environ.get("PG_PASSWORD", "Vasv@9344")
    database: str = os.environ.get("PG_DATABASE", "cdc_demo")
