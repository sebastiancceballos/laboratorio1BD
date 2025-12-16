import os
from urllib.parse import urlparse

import pymysql
from dotenv import load_dotenv

# Load .env
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL not set in environment")

# Expect format: mysql+pymysql://user:pass@host:port/dbname
parsed = urlparse(DATABASE_URL.replace("+pymysql", ""))
user = parsed.username or "root"
password = parsed.password or ""
host = parsed.hostname or "localhost"
port = int(parsed.port or 3306)
db_name = parsed.path.lstrip("/") or "lb_bd"

# Connect without selecting a DB and create if not exists
conn = pymysql.connect(host=host, user=user, password=password, port=port, cursorclass=pymysql.cursors.Cursor, autocommit=True)
try:
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4;")
        print(f"Database ensured: {db_name}")
finally:
    conn.close()
