from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


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
