import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('PG_HOST'),
    port=os.getenv('PG_PORT'),
    user=os.getenv('PG_USER'),
    password=os.getenv('PG_PASSWORD'),
    database=os.getenv('PG_DATABASE'),
)
cur = conn.cursor()

cur.execute('''
    SELECT
        slot_name,
        pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
        active
    FROM pg_replication_slots
''')

print('slot_name | retained_wal | active')
print('-' * 45)
for row in cur.fetchall():
    print(f'{row[0]} | {row[1]} | {row[2]}')

conn.close()
