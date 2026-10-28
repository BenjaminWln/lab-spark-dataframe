## Spark and S3

**What happens to a running Spark job if the service restarts, and why?**

The job stops because the service running the Spark driver and executors is stopped. The new service gets new temporary credentials, so Spark must start a new session with them.

**Why does `SimpleAWSCredentialsProvider` fail when a session token is used?**

It uses only the access key and secret key. Temporary credentials also need a session token, which `TemporaryAWSCredentialsProvider` reads.

## Spark SQL

**Explain the output of the code that was just run.**

The first output shows five deduplicated orders. The checks print nothing because the DataFrame and SQL results match. The last query counts users by state and counts missing cities. The military address groups have missing cities because those addresses do not include a city.

**Which would you choose for one-off work and for a model shared with colleagues: DataFrame API or SQL? Why?**

For one-off work, I would use SQL because it is quick to write and read. For a model shared with colleagues, I would use the DataFrame API because Python functions are easier to reuse and test. A team that mainly uses SQL could choose SQL instead.

**Where else could an empty string from `regexp_extract()` cause a wrong result?**

It could affect the `state` and `zip_code` columns. An unmatched address could create empty values instead of `NULL`, which could appear as a false group in a query.

## Parquet output

**What is written under `silver/users.parquet/`? How many part-files are there, and why?**

There is one part-file for each output partition, plus a `_SUCCESS` marker. The users CSV is read as one partition because it has multiline records, so it produces one part-file. The number of orders part-files depends on the shuffle partitions and Spark's settings.

**What is lost by using `coalesce(1)` before writing a large dataset?**

The write loses parallelism because one task must write all the data. It can be slow or use too much memory. The resulting single file also gives less parallelism when it is read later.

## Iceberg tables

**In `silver/orders.parquet/` and `warehouse/silver/orders/`, which files hold the data and which describe the table?**

The Parquet directory contains data files. In the Iceberg directory, the `data/` folder contains Parquet files, and the `metadata/` folder contains information about the table and its snapshots.

**What can happen if a job overwrites a Parquet directory while it is being read? What changes with Iceberg?**

A reader may see missing or mixed files while the directory changes. Iceberg commits changes as a new snapshot, so readers can continue to use a consistent table version.

**What can go wrong if two jobs update the version pointer at the same time? What can a catalog service add?**

One job may overwrite the other job's pointer, so a commit can be lost or the table can point to the wrong version. A catalog service can coordinate commits and prevent conflicting updates.

## Compare with the dbt lab

**How do the schemas differ? Spark has no UUID type. What was done, and what does this mean for consumers?**

The ID columns are stored as strings in Spark, not as UUIDs. Consumers cannot rely on the database to check that these strings are valid UUIDs, and they may need to convert them before comparing them with dbt data.

**Which output layout is easier to read, and which one scales better?**

A single dbt output file is easier to open. Spark's directory of part-files scales better because many tasks can write and read files in parallel.

**What did this lab use instead of dbt's data tests? What is needed for the same checks?**

This lab used manual checks and assertions. To get the same guarantees, add tests for unique values, missing values, allowed values and matching IDs, then run them after each build.

**What gives the notebook the dependency order that `ref()` and `source()` give dbt?**

The order of the notebook cells and the DataFrames passed between them define the dependencies. The notebook does not have a separate model dependency graph.

**Why are the dbt and Spark build times not directly comparable?**

dbt creates views and runs tests, while Spark processes and writes the data. Spark also has start-up and file-writing work. The two runs do different tasks and have different fixed costs.

**Would you choose dbt with DuckDB or Spark for each case: 10 GB per day, 10 TB per day, or a team that only knows SQL?**

- For 10 GB per day, I would choose dbt with DuckDB because one machine can often handle this size.
- For 10 TB per day, I would choose Spark because it can spread the work across a cluster.
- For a team that only knows SQL, I would choose dbt. If the data needs Spark, the team can still use dbt with Spark SQL.

## Exercises

### 1. `birthdate_is_valid`

The column is `False` when a user's birthdate is later than their first order. The DataFrame and SQL versions return the same results. Unlike the dbt solution, this exercise keeps the original birthdate and adds a validity flag.

### 2. ZIP code extraction

The Python UDF, `pandas_udf` and `regexp_extract()` return the same ZIP codes. `regexp_extract()` is usually the best choice because it runs in Spark without sending rows to Python. A `pandas_udf` handles data in batches and is usually faster than a regular Python UDF on larger datasets.

### 3. Cache the bronze DataFrames

Without caching, each action reads the CSV files again. After caching, later actions can reuse the DataFrames. The first action may take longer because it also stores the data.

### 4. Iceberg append and time travel

An Iceberg append creates a new snapshot and keeps the previous one. `VERSION AS OF` can read the table before the append. A plain Parquet directory does not keep earlier versions, so its previous data cannot be read unless it was saved elsewhere.
