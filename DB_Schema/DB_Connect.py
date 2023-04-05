from sqlalchemy import create_engine, text, exc
from sqlalchemy.orm import sessionmaker

engine = create_engine('postgresql://postgres:tanmay@localhost/loop', pool_size=15, max_overflow=5)
session = sessionmaker(autocommit=False, autoflush=False, bind = engine)
