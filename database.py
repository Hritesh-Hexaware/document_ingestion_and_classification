from sqlalchemy import create_engine, MetaData, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
import logging
from urllib.parse import quote_plus
import psycopg2

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Azure Cosmos DB for PostgreSQL connection settings
DB_HOST = os.getenv("AZURE_POSTGRESQL_HOST")
DB_NAME = os.getenv("AZURE_POSTGRESQL_DATABASE")
DB_USER = os.getenv("AZURE_POSTGRESQL_USER")
DB_PASSWORD = os.getenv("AZURE_POSTGRESQL_PASSWORD")
DB_PORT = os.getenv("AZURE_POSTGRESQL_PORT", "5432")

# URL encode the password to handle special characters
encoded_password = quote_plus(DB_PASSWORD) if DB_PASSWORD else None


if not all([DB_HOST, DB_NAME, DB_USER, encoded_password]):
    missing_vars = [var for var, val in {
        "AZURE_POSTGRESQL_HOST": DB_HOST,
        "AZURE_POSTGRESQL_DATABASE": DB_NAME,
        "AZURE_POSTGRESQL_USER": DB_USER,
        "AZURE_POSTGRESQL_PASSWORD": encoded_password
    }.items() if not val]
    raise ValueError(f"Missing required database environment variables: {', '.join(missing_vars)}")

try:
    # First create schema using psycopg2
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode='require'
    )
    conn.autocommit = True
    
    with conn.cursor() as cursor:
        # Create schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS requirementsbits")
        logger.info("Schema 'requirementsbits' created or already exists")
        
        # Set search_path
        cursor.execute("SET search_path TO requirementsbits")
        logger.info("Successfully set search_path to requirementsbits")
    
    conn.close()
    logger.info("Successfully initialized schema")
except Exception as e:
    logger.error(f"Error initializing schema: {str(e)}")
    raise

# Construct the connection URL for Azure Cosmos DB for PostgreSQL with encoded password
SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine with Cosmos DB specific configurations
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={
        "sslmode": "require"
    },
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_pre_ping=True
)

# Create schema-aware MetaData
metadata = MetaData(schema="requirementsbits")

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create Base class with schema-aware metadata
Base = declarative_base(metadata=metadata)

# Event to set search_path for each connection
@event.listens_for(engine, "connect")
def set_search_path(dbapi_connection, connection_record):
    with dbapi_connection.cursor() as cursor:
        cursor.execute("SET search_path TO requirementsbits")
    logger.info("Set search_path for new connection")

# Add event listeners for debugging
@event.listens_for(engine, "connect")
def receive_connect(dbapi_connection, connection_record):
    logger.info("New database connection established")

@event.listens_for(engine, "checkout")
def receive_checkout(dbapi_connection, connection_record, connection_proxy):
    logger.info("Database connection checked out from pool")

# Dependency to get database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 