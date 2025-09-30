#!/usr/bin/env python3
"""Run performance benchmark on Galera Cluster."""

import os
import time
import logging
import statistics
from dotenv import load_dotenv
import mysql.connector
import random
from concurrent.futures import ThreadPoolExecutor

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
NUM_THREADS = int(os.getenv("BENCHMARK_THREADS"))
QUERIES_PER_THREAD = int(os.getenv("BENCHMARK_QUERIES_PER_THREAD"))

DB_NAMES = [f"{DB_PREFIX}test{i}" for i in range(1, NUM_DATABASES + 1)]

def run_benchmark_thread(thread_id):
    try:
        # Select a random database from the list
        db_name = random.choice(DB_NAMES)
        
        # Connect to database
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            database=db_name
        )
        logger.debug(f"Thread {thread_id} connected to {db_name}")
        cursor = connection.cursor(dictionary=True)
        
        query_times = []
        errors = 0
        
        # Run queries
        for i in range(QUERIES_PER_THREAD):
            # Choose query type
            query_type = random.choice(["SELECT", "INSERT", "UPDATE"])
            
            try:
                start_time = time.time()
                
                if query_type == "SELECT":
                    # Simple SELECT query
                    cursor.execute("SELECT * FROM users LIMIT 10")
                    results = cursor.fetchall()
                
                elif query_type == "INSERT":
                    # Simple INSERT query
                    username = f"user_{thread_id}_{i}"
                    email = f"{username}@example.com"
                    cursor.execute(
                        "INSERT INTO users (username, email, first_name, last_name, status) VALUES (%s, %s, %s, %s, %s)",
                        (username, email, "First", "Last", "active")
                    )
                    connection.commit()
                
                elif query_type == "UPDATE":
                    # Simple UPDATE query
                    cursor.execute("UPDATE users SET status = 'active' WHERE id = 1")
                    connection.commit()
                
                execution_time = time.time() - start_time
                query_times.append((query_type, execution_time))
                
                # Small delay to avoid hammering the database
                time.sleep(0.1)
            
            except Exception as e:
                logger.error(f"Thread {thread_id} error: {e}")
                errors += 1
        
        # Close database connection
        cursor.close()
        connection.close()
        
        return query_times, errors
    
    except Exception as e:
        logger.error(f"Thread {thread_id} connection error: {e}")
        return [], 1

def main():
    logger.info(f"Starting benchmark with {NUM_THREADS} threads")
    logger.info(f"Testing against {NUM_DATABASES} database(s): {', '.join(DB_NAMES)}")
    logger.info(f"Each thread will execute {QUERIES_PER_THREAD} queries")
    
    start_time = time.time()
    
    # Run threads
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(run_benchmark_thread, i) for i in range(NUM_THREADS)]
        
        # Wait for all threads to complete
        all_query_times = {}
        total_errors = 0
        
        for future in futures:
            query_times, errors = future.result()
            total_errors += errors
            
            # Group query times by type
            for query_type, execution_time in query_times:
                if query_type not in all_query_times:
                    all_query_times[query_type] = []
                all_query_times[query_type].append(execution_time)
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    # Calculate statistics
    total_queries = sum(len(times) for times in all_query_times.values())
    queries_per_second = total_queries / total_duration if total_duration > 0 else 0
    
    # Generate report
    logger.info("Benchmark complete")
    logger.info(f"Total duration: {total_duration:.2f} seconds")
    logger.info(f"Total queries: {total_queries}")
    logger.info(f"Total errors: {total_errors}")
    logger.info(f"Queries per second: {queries_per_second:.2f}")
    
    for query_type, times in all_query_times.items():
        if times:
            avg_time = statistics.mean(times)
            median_time = statistics.median(times)
            min_time = min(times)
            max_time = max(times)
            
            logger.info(f"{query_type} queries:")
            logger.info(f"  Count: {len(times)}")
            logger.info(f"  Average time: {avg_time:.6f} seconds")
            logger.info(f"  Median time: {median_time:.6f} seconds")
            logger.info(f"  Min time: {min_time:.6f} seconds")
            logger.info(f"  Max time: {max_time:.6f} seconds")

if __name__ == "__main__":
    main()
