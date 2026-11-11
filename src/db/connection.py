"""
Database Connection & Session Management
Handles SQLAlchemy engine creation, table initialization, and session scopes.
"""
from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from src.config import DATABASE_URL
from src.db.models import Base


def get_engine(db_url: str | None = None):
    """
    Creates and returns a SQLAlchemy engine.
    Supports PostgreSQL, SQLite (for memory/testing), and others.
    """
    url = db_url or DATABASE_URL
    connect_args = {}
    if url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(url, connect_args=connect_args, pool_pre_ping=True)


def init_db(engine=None):
    """
    Initializes database tables defined in the SQLAlchemy models.
    """
    if engine is None:
        engine = get_engine()
    Base.metadata.create_all(bind=engine)
    return engine


@contextmanager
def get_session(engine=None) -> Generator[Session, None, None]:
    """
    Context manager for database sessions, ensuring commits on success and rollback on error.
    """
    if engine is None:
        engine = get_engine()
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session: Session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
