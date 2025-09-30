# MariaDB Benchmark Tool

Simple performance benchmarking tool for MariaDB databases.

## Features

- Generate test databases with realistic data
- Run performance benchmarks (SELECT, INSERT, UPDATE)
- Detailed performance reports
- Easy cleanup of test databases

## Requirements

- Python 3.8+
- MariaDB 10.11+ (standalone or Galera Cluster)
- For Galera Cluster: HAProxy recommended for load distribution
- Dependencies from `requirements.txt`

## Installation

1. Clone repository
2. Create virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create database user on your MariaDB server/cluster:
   ```sql
   CREATE USER 'benchmark_user'@'%' IDENTIFIED BY 'your_secure_password';
   GRANT ALL PRIVILEGES ON `gbench_%`.* TO 'benchmark_user'@'%';
   FLUSH PRIVILEGES;
   ```
   **Note:** For Galera Cluster, create this user on one node - it will replicate automatically.
   
5. Copy `.env.example` to `.env` and configure:
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   # For Galera Cluster: Use HAProxy VIP as DB_HOSTS
   ```

## Usage

### 1. Generate Test Data

```bash
python generate_testdata.py
```

Creates databases with tables (`users`, `products`, `orders`, `order_items`) and test data.

### 2. Run Benchmark

```bash
python run_benchmark.py
```

Runs performance tests with SELECT, INSERT, and UPDATE queries. Reports QPS and latency statistics.

### 3. Cleanup

```bash
python cleanup_databases.py
```

Removes all benchmark databases.

## Configuration

Edit `.env` file:

```bash
# Database connection
DB_HOSTS=192.168.x.x          # Single server IP or HAProxy VIP for Galera Cluster
DB_USER=benchmark_user
DB_PASSWORD=your_secure_password
DB_PORT=3306

# Test data generation
BENCHMARK_DB_PREFIX=gbench_
BENCHMARK_NUM_DATABASES=3
BENCHMARK_NUM_USERS=1000
BENCHMARK_NUM_PRODUCTS=2000
BENCHMARK_NUM_ORDERS=1500
BENCHMARK_NUM_ORDER_ITEMS=3000

# Benchmark settings
BENCHMARK_THREADS=50              # Concurrent threads (adjust based on your cluster)
BENCHMARK_QUERIES_PER_THREAD=100
```

**For Galera Cluster with HAProxy:**
- `DB_HOSTS` should point to the HAProxy VIP
- HAProxy distributes load across cluster nodes
- Benchmark tests all databases with random distribution

## Example Output

```
Starting benchmark with 10 threads
Testing against 3 database(s): gbench_test1, gbench_test2, gbench_test3
Each thread will execute 100 queries
Benchmark complete
Total duration: 16.04 seconds
Total queries: 1000
Total errors: 0
Queries per second: 62.34

SELECT queries:
  Count: 337
  Average time: 0.032342 seconds
  Median time: 0.032121 seconds

UPDATE queries:
  Count: 340
  Average time: 0.061706 seconds

INSERT queries:
  Count: 323
  Average time: 0.073277 seconds
```

## License

MIT
