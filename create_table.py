import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="mysecretpassword"
)

cur = conn.cursor()

create_scenarios_query = """
CREATE TABLE IF NOT EXISTS scenarios (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    vid_id INT,
    predict TEXT,
    runner_id INT
);
"""



try:
    cur.execute(create_scenarios_query)

    conn.commit()
    print("+")
except Exception as e:
    print(f"err: {e}")
finally:
    cur.close()
    conn.close()
