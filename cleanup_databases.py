#!/usr/bin/env python3
"""Remove benchmark databases."""

import os
import logging
from dotenv import load_dotenv
import mysql.connector

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOSTS").split(",")[0]
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT"))
DB_PREFIX = os.getenv("BENCHMARK_DB_PREFIX")

def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        logger.info(f"Connected to {DB_HOST}")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def list_databases(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("SHOW DATABASES")
        databases = [row[0] for row in cursor]
        return databases
    except Exception as e:
        logger.error(f"Error listing databases: {e}")
        raise
    finally:
        cursor.close()

def drop_database(connection, db_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        logger.info(f"Dropped database {db_name}")
    except Exception as e:
        logger.error(f"Error dropping database {db_name}: {e}")
    finally:
        cursor.close()

def main():
    # Connect to the database
    connection = connect_to_db()
    
    # List all databases
    databases = list_databases(connection)
    
    # Find benchmark databases
    benchmark_dbs = [db for db in databases if db.startswith(DB_PREFIX)]
    
    if not benchmark_dbs:
        logger.info(f"No databases found with prefix {DB_PREFIX}")
        return
    
    logger.info(f"Found {len(benchmark_dbs)} benchmark databases:")
    for db in benchmark_dbs:
        logger.info(f"  - {db}")
    
    # Confirm deletion
    confirm = input(f"Do you want to drop these {len(benchmark_dbs)} databases? (y/N): ")
    if confirm.lower() != 'y':
        logger.info("Cleanup cancelled")
        return
    
    # Drop benchmark databases
    for db in benchmark_dbs:
        drop_database(connection, db)
    
    # Close the connection
    connection.close()
    logger.info("Cleanup completed")

if __name__ == "__main__":
    main()
