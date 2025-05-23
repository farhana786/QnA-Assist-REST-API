import os
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy.pool import QueuePool
from sqlalchemy import create_engine
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
import logging

load_dotenv()

# Initialize the custom logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sf_key = os.getenv("SNOWFLAKE_PRIVATE_KEY")
private_key_encoded = sf_key.encode()

private_key_passphrase = os.getenv("daas_edp_sf_key_passphrase")
private_key_passphrase_encoded = private_key_passphrase.encode()

private_key_loaded = serialization.load_pem_private_key(
    private_key_encoded,
    password=private_key_passphrase_encoded,
    backend=default_backend(),
)

private_key_serialized = private_key_loaded.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')

def get_conn():
    try:
        return snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
            private_key=private_key_serialized,            
            client_session_keep_alive=True
        )
    except Exception as e:
        logger.error(f"Error establishing Snowflake connection: {str(e)}")
        raise

pool = QueuePool(get_conn, max_overflow=int(os.getenv('SF_POOL_MAX_OVERFLOW', 10)), pool_size=int(os.getenv('SF_POOL_SIZE', 5)), timeout=float(os.getenv('SF_POOL_TIMEOUT', 30)))

def get_snowflake_connection():
    try:
        conn = pool.connect()
        logger.info("Snowflake connection established successfully.")
        return conn
    except Exception as e:
        logger.error(f"Failed to get connection from Snowflake pool: {str(e)}")
        raise
