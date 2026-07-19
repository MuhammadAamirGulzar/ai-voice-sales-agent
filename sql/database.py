import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

load_dotenv()

SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./schema.db")

_connect_args = {}
if SQLALCHEMY_DATABASE_URL.startswith("sqlite"):
    _connect_args["check_same_thread"] = False

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args=_connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def ensure_columns():
    """
    Minimal additive migration: create_all() only creates missing tables,
    never new columns on existing ones. Adds columns the streaming voice
    engine needs to databases created before it existed. Safe to run on
    every startup.
    """
    from sqlalchemy import inspect, text

    json_type = "JSONB" if engine.dialect.name == "postgresql" else "JSON"
    wanted = {
        "chat_history": [("metrics", json_type)],
    }
    inspector = inspect(engine)
    with engine.begin() as conn:
        for table, columns in wanted.items():
            if table not in inspector.get_table_names():
                continue
            existing = {c["name"] for c in inspector.get_columns(table)}
            for name, col_type in columns:
                if name not in existing:
                    conn.execute(text(
                        f"ALTER TABLE {table} ADD COLUMN {name} {col_type}"))
                    print(f"[db] added column {table}.{name}")
