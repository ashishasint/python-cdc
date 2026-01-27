import struct
import logging
from typing import Optional, Dict
from utils.cdc_event import CDCEvent

logger = logging.getLogger(__name__)


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
