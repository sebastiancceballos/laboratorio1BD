import os
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

# Default to local MySQL database (adjust in your .env)
DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "mysql+pymysql://user:password@localhost:3306/lb_db",
)
IS_SQLITE: bool = DATABASE_URL.startswith("sqlite")
