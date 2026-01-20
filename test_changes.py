import psycopg2
import time


def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        user="postgres",
        password="Vasv@9344",
        database="cdc_demo",
    )


def test_insert():
    conn = get_connection()
    cur = conn.cursor()

    print("📝 Inserting new user...")
    cur.execute(
        """
        INSERT INTO users (name, email, status) 
        VALUES ('Test User', 'test@example.com', 'active')
        RETURNING id
    """
    )
    user_id = cur.fetchone()[0]
    conn.commit()
    print(f"   Created user with ID: {user_id}")

    cur.close()
    conn.close()
    return user_id


def test_update(user_id):
    conn = get_connection()
    cur = conn.cursor()

    print(f"✏️ Updating user {user_id}...")
    cur.execute(
        """
        UPDATE users 
        SET name = 'Updated User', status = 'inactive', updated_at = NOW()
        WHERE id = %s
    """,
        (user_id,),
    )
    conn.commit()
    print("   Update complete")

    cur.close()
    conn.close()


def test_delete(user_id):
    conn = get_connection()
    cur = conn.cursor()

    print(f"🗑️ Deleting user {user_id}...")
    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
    conn.commit()
    print("   Delete complete")

    cur.close()
    conn.close()


def main():
    print("\n" + "=" * 50)
    print("Starting CDC Test Sequence")
    print("=" * 50 + "\n")

    # Test INSERT
    user_id = test_insert()
    time.sleep(1)

    # Test UPDATE
    test_update(user_id)
    time.sleep(1)

    # Test DELETE
    test_delete(user_id)

    print("\n" + "=" * 50)
    print("Test sequence complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()
