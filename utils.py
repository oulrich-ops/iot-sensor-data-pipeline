import psycopg2
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection(host, user, password, db_name):
    """Connect to PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            connect_timeout=5
        )
        logger.info(f"Connected to PostgreSQL: {host}/{db_name}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise