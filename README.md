# PerformanceTest Spring Batch Project

This repository contains a **Spring Batch** application demonstrating various batch processing strategies:

-   `simpleTransactionJob`: Single-threaded step processing using `JdbcPagingItemReader` → `ItemProcessor` → `FlatFileItemWriter`.
-   `multiThreadedJob`: Parallel chunk processing using a `TaskExecutor` in a single step.
-   `asyncProcessingJob`: Asynchronous offloading of the `ItemProcessor` via `AsyncItemProcessor`/`AsyncItemWriter`.
-   `partitionedJob`: Master/worker partitioning of a large table across multiple threads or nodes.

Full implementation lives in the Git branch: `full project`.

---

## Prerequisites

-   Java 17+ (compatible with Spring Boot 3.x)
-   Maven or Gradle
-   A running **PostgreSQL** (or other JDBC) database with a table named `transactions(id, transaction_date, amount, created_at)` populated with your data.

---

## Configuration

All batch parameters and job selection are externalized under `application.yml` or `application.properties`.

```yaml
# application.yml
app:
  # Select which job to run: simpleTransactionJob, multiThreadedJob,
  # asyncProcessingJob, or partitionedJob
  job:
    name: partitionedJob

batch:
  # Chunk size for steps
  chunk-size: 5000
  # Page size for JdbcPagingItemReader
  page-size: 5000
  # Number of threads for multi-thread & async steps
  core-pool-size: 6
  # Number of partitions for partitionedJob
  partition-size: 8
