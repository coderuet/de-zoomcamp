from dotenv import load_dotenv
import os

load_dotenv()


class Config:
    MAIN_DB_PORT = os.getenv("MAIN_DB_PORT")  # From .env
    MAIN_DB_USERNAME = os.getenv("MAIN_DB_USERNAME")
    MAIN_DB_PASSWORD = os.getenv("MAIN_DB_PASSWORD")
    MAIN_DB_DATABASE_NAME = os.getenv(
        "MAIN_DB_DATABASE_NAME",
    )
    MAIN_DB_HOST = os.getenv("MAIN_DB_HOST", default="localhost")
