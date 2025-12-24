from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy import create_engine
from config import settings

engine = create_engine(
    url=settings.DATABASE_URL,
    echo=False
)

session_factory = sessionmaker(engine)

class Base(DeclarativeBase):
    pass