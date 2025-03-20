import psycopg2

try:
    conn = psycopg2.connect(
        host="cr.chie24ys0emx.us-east-1.rds.amazonaws.com",
        port="5432",
        user="CR1",
        password="Crave0413*",
        dbname="craverank"
    )
    print("Database connection successful!")
    conn.close()
except Exception as e:
    print(f"Database connection error: {e}")
