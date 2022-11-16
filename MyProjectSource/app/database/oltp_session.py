import os
from sqlalchemy import create_engine  
from sqlalchemy.orm import sessionmaker


host = os.environ["POSTGRES_HOST"]
port = os.environ["POSTGRES_PORT"]
user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASS"]
db = os.environ["POSTGRES_OLTP"]
dbtype = "postgresql+psycopg2"

SQLALCHEMY_DATABASE_URI = f"{dbtype}://{user}:{password}@{host}:{port}/{db}"

oltpEngine = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
oltpSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=oltpEngine)
