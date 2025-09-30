#!/usr/bin/env python3
"""Generate test databases with realistic data."""

import os
import logging
from dotenv import load_dotenv
import mysql.connector
from faker import Faker
import random

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

NUM_DATABASES = int(os.getenv("BENCHMARK_NUM_DATABASES"))
NUM_USERS = int(os.getenv("BENCHMARK_NUM_USERS"))
NUM_PRODUCTS = int(os.getenv("BENCHMARK_NUM_PRODUCTS"))
NUM_ORDERS = int(os.getenv("BENCHMARK_NUM_ORDERS"))
NUM_ORDER_ITEMS = int(os.getenv("BENCHMARK_NUM_ORDER_ITEMS"))

fake = Faker()

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

def create_database(connection, db_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        cursor.execute(f"CREATE DATABASE {db_name}")
        logger.info(f"Created database {db_name}")
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        raise
    finally:
        cursor.close()

def create_tables(connection, db_name):
    """Create tables."""
    cursor = connection.cursor()
    try:
        # Use the database
        cursor.execute(f"USE {db_name}")
        
        # Create users table
        cursor.execute("""
        CREATE TABLE users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(50) NOT NULL,
            email VARCHAR(100) NOT NULL,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status ENUM('active', 'inactive', 'suspended') DEFAULT 'active',
            INDEX idx_email (email),
            INDEX idx_status (status)
        )
        """)
        logger.info(f"Created table {db_name}.users")
        
        # Create products table
        cursor.execute("""
        CREATE TABLE products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            price DECIMAL(10,2) NOT NULL,
            category VARCHAR(50),
            stock INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_category (category),
            INDEX idx_price (price)
        )
        """)
        logger.info(f"Created table {db_name}.products")
        
        # Create orders table
        cursor.execute("""
        CREATE TABLE orders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
            total_amount DECIMAL(10,2) NOT NULL,
            shipping_address TEXT,
            payment_method VARCHAR(50),
            INDEX idx_user_id (user_id),
            INDEX idx_status (status),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """)
        logger.info(f"Created table {db_name}.orders")
        
        # Create order_items table
        cursor.execute("""
        CREATE TABLE order_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL DEFAULT 1,
            price DECIMAL(10,2) NOT NULL,
            INDEX idx_order_id (order_id),
            INDEX idx_product_id (product_id),
            FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
        )
        """)
        logger.info(f"Created table {db_name}.order_items")
        
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise
    finally:
        cursor.close()

def insert_data(connection, db_name, table_name, data_list, batch_size=1000):
    if not data_list:
        return []
    
    cursor = connection.cursor()
    try:
        # Use the database
        cursor.execute(f"USE {db_name}")
        
        # Prepare the insert query
        columns = ", ".join(data_list[0].keys())
        placeholders = ", ".join(["%s"] * len(data_list[0]))
        
        # Insert data in batches
        inserted_ids = []
        
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            
            # Build multi-row insert query
            values_list = []
            params = []
            for data in batch:
                values_list.append(f"({placeholders})")
                params.extend(data.values())
            
            query = f"INSERT INTO {table_name} ({columns}) VALUES {', '.join(values_list)}"
            cursor.execute(query, params)
            
            # Get the first ID of this batch
            first_id = cursor.lastrowid
            
            # Generate IDs for this batch (auto_increment IDs are sequential)
            for j in range(len(batch)):
                inserted_ids.append(first_id + j)
        
        # Commit the transaction
        connection.commit()
        
        # Verify the IDs by querying the table
        cursor.execute(f"SELECT id FROM {table_name} ORDER BY id")
        actual_ids = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Inserted {len(actual_ids)} rows into {db_name}.{table_name}")
        return actual_ids
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {e}")
        raise
    finally:
        cursor.close()

def generate_user_data(count=10):
    users = []
    for _ in range(count):
        users.append({
            "username": fake.user_name(),
            "email": fake.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "status": random.choice(["active", "inactive", "suspended"])
        })
    return users

def generate_product_data(count=20):
    products = []
    for _ in range(count):
        products.append({
            "name": fake.catch_phrase(),
            "description": fake.text(max_nb_chars=200),
            "price": round(random.uniform(5, 1000), 2),
            "category": random.choice([
                "Electronics", "Clothing", "Books", "Home", "Sports",
                "Beauty", "Toys", "Food", "Health", "Automotive"
            ]),
            "stock": random.randint(0, 1000)
        })
    return products

def generate_order_data(count=15, user_ids=None):
    if not user_ids:
        user_ids = [1]
    
    orders = []
    for _ in range(count):
        orders.append({
            "user_id": random.choice(user_ids),
            "status": random.choice([
                "pending", "processing", "shipped", "delivered", "cancelled"
            ]),
            "total_amount": round(random.uniform(10, 5000), 2),
            "shipping_address": fake.address(),
            "payment_method": random.choice([
                "credit_card", "paypal", "bank_transfer", "cash_on_delivery"
            ])
        })
    return orders

def generate_order_item_data(count=30, order_ids=None, product_ids=None):
    if not order_ids:
        order_ids = [1]
    if not product_ids:
        product_ids = [1]
    
    order_items = []
    for _ in range(count):
        quantity = random.randint(1, 10)
        price = round(random.uniform(5, 500), 2)
        order_items.append({
            "order_id": random.choice(order_ids),
            "product_id": random.choice(product_ids),
            "quantity": quantity,
            "price": price
        })
    return order_items

def main():
    # Connect to the database
    connection = connect_to_db()
    
    logger.info(f"Creating {NUM_DATABASES} database(s) with test data...")
    
    # Create multiple databases
    for db_num in range(1, NUM_DATABASES + 1):
        db_name = f"{DB_PREFIX}test{db_num}"
        
        logger.info(f"Creating database {db_num}/{NUM_DATABASES}: {db_name}")
        
        # Create database
        create_database(connection, db_name)
        
        # Create tables
        create_tables(connection, db_name)
        
        # Insert data
        logger.info(f"  Inserting {NUM_USERS} users...")
        user_ids = insert_data(connection, db_name, "users", generate_user_data(NUM_USERS))
        
        logger.info(f"  Inserting {NUM_PRODUCTS} products...")
        product_ids = insert_data(connection, db_name, "products", generate_product_data(NUM_PRODUCTS))
        
        logger.info(f"  Inserting {NUM_ORDERS} orders...")
        order_ids = insert_data(connection, db_name, "orders", generate_order_data(NUM_ORDERS, user_ids))
        
        logger.info(f"  Inserting {NUM_ORDER_ITEMS} order items...")
        insert_data(connection, db_name, "order_items", generate_order_item_data(NUM_ORDER_ITEMS, order_ids, product_ids))
        
        logger.info(f"Database {db_name} completed successfully")
    
    # Close the connection
    connection.close()
    logger.info(f"All {NUM_DATABASES} database(s) created successfully")

if __name__ == "__main__":
    main()
