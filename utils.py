import psycopg2


def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        user="postgres",
        password="Vasv@9344",
        database="cdc_demo",
    )


def check_wal_level():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SHOW wal_level")
    result = cur.fetchone()[0]
    print(f"WAL Level: {result}")
    cur.close()
    conn.close()
    return result


def list_replication_slots():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT slot_name, plugin, slot_type, active 
        FROM pg_replication_slots
    """
    )
    slots = cur.fetchall()
    print("\nReplication Slots:")
    print("-" * 60)
    for slot in slots:
        print(
            f"  Name: {slot[0]}, Plugin: {slot[1]}, Type: {slot[2]}, Active: {slot[3]}"
        )
    if not slots:
        print("  No slots found")
    cur.close()
    conn.close()
    return slots


def list_publications():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pg_publication")
    pubs = cur.fetchall()
    print("\nPublications:")
    print("-" * 60)
    for pub in pubs:
        print(f"  {pub}")
    cur.close()
    conn.close()


def list_publication_tables():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pg_publication_tables")
    tables = cur.fetchall()
    print("\nPublication Tables:")
    print("-" * 60)
    for table in tables:
        print(f"  Publication: {table[0]}, Schema: {table[1]}, Table: {table[2]}")
    cur.close()
    conn.close()


def drop_slot(slot_name):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT pg_drop_replication_slot('{slot_name}')")
        conn.commit()
        print(f"Dropped slot: {slot_name}")
    except Exception as e:
        print(f"Error dropping slot: {e}")
    cur.close()
    conn.close()


def show_users():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users")
    users = cur.fetchall()
    print("\nUsers Table:")
    print("-" * 60)
    for user in users:
        print(f"  {user}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("PostgreSQL CDC Status Check")
    print("=" * 60)

    check_wal_level()
    list_replication_slots()
    list_publications()
    list_publication_tables()
    show_users()
