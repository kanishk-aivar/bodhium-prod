### SQL Database Documentation, Creation, and Indexing

This document outlines the schema, purpose, and SQL commands for creating and optimizing your database tables: `scrapejobs`, `products`, `queries`, and `llmtasks`.

-----

#### 1\. `scrapejobs` Table

**Documentation 📜**
The `scrapejobs` table is designed to track individual web scraping operations. It stores metadata about each job, including its source, current status, and timestamps for creation, updates, and completion. It also captures any errors encountered.

| Column Name | Data Type | Nullable | Default Value | Description |
| :--- | :--- | :--- | :--- | :--- |
| `job_id` | `uuid` | `NO` | `gen_random_uuid()` | **Primary Key.** Unique identifier for each scraping job. |
| `source_url` | `text` | `YES` | | The URL or starting point for the scraping job. |
| `status` | `character varying` | `YES` | | Current status of the job (e.g., 'pending', 'in\_progress', 'completed', 'failed'). |
| `created_at` | `timestamp with time zone` | `YES` | `CURRENT_TIMESTAMP` | Timestamp when the job was initially created. |
| `updated_at` | `timestamp with time zone` | `YES` | `CURRENT_TIMESTAMP` | Last timestamp when the job's record was modified. |
| `brand_name` | `character varying` | `YES` | | The brand associated with the scraping task. |
| `error_message` | `text` | `YES` | | Detailed message if the job failed. |
| `completed_at` | `timestamp with time zone` | `YES` | | Timestamp when the job successfully completed. |
| `progress` | `jsonb` | `YES` | `'{}'::jsonb` | JSON object storing dynamic progress information. |

**Table Creation 🛠️**

```sql
CREATE TABLE scrapejobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_url TEXT,
    status CHARACTER VARYING,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    brand_name CHARACTER VARYING,
    error_message TEXT,
    completed_at TIMESTAMP WITH TIME ZONE,
    progress JSONB DEFAULT '{}'::jsonb
);
```

**Indexing Commands ⚡**

```sql
-- Index on status for quickly finding jobs by their current state
CREATE INDEX idx_scrapejobs_status ON scrapejobs (status);

-- Index on created_at for chronological sorting and range queries
CREATE INDEX idx_scrapejobs_created_at ON scrapejobs (created_at);

-- Index on brand_name for filtering jobs by specific brands
CREATE INDEX idx_scrapejobs_brand_name ON scrapejobs (brand_name);
```

-----

#### 2\. `products` Table

**Documentation 📜**
The `products` table stores the detailed information about individual products scraped from various sources. It includes unique identifiers, raw product data, and a link back to the scraping job that discovered it.

| Column Name | Data Type | Nullable | Default Value | Description |
| :--- | :--- | :--- | :--- | :--- |
| `product_id` | `bigint` | `NO` | `nextval('products_product_id_seq')` | **Primary Key.** Unique identifier for each product. |
| `product_hash` | `character varying` | `YES` | | A hash value to identify unique product data. |
| `product_data` | `jsonb` | `YES` | | JSON object containing the scraped product's details. |
| `source_url` | `text` | `YES` | | The URL from which the product data was scraped. |
| `first_scraped_at` | `timestamp with time zone` | `YES` | `CURRENT_TIMESTAMP` | Timestamp when the product was first scraped. |
| `brand_name` | `character varying` | `YES` | | The brand associated with this product. |
| `job_id` | `uuid` | `YES` | | **Foreign Key** referencing `scrapejobs.job_id`. Links the product to the job that found it. |

**Table Creation 🛠️**

```sql
CREATE SEQUENCE IF NOT EXISTS products_product_id_seq;

CREATE TABLE products (
    product_id BIGINT PRIMARY KEY DEFAULT nextval('products_product_id_seq'),
    product_hash CHARACTER VARYING,
    product_data JSONB,
    source_url TEXT,
    first_scraped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    brand_name CHARACTER VARYING,
    job_id UUID,
    CONSTRAINT fk_job
        FOREIGN KEY (job_id)
        REFERENCES scrapejobs (job_id)
        ON DELETE SET NULL
);
```

**Indexing Commands ⚡**

```sql
-- Unique index on product_hash to ensure data integrity and fast lookups
CREATE UNIQUE INDEX uix_products_product_hash ON products (product_hash);

-- Index on brand_name for filtering products by brand
CREATE INDEX idx_products_brand_name ON products (brand_name);

-- Index on job_id for efficient foreign key lookups and joins with scrapejobs
CREATE INDEX idx_products_job_id ON products (job_id);

-- Index on first_scraped_at for chronological queries
CREATE INDEX idx_products_first_scraped_at ON products (first_scraped_at);
```

-----

#### 3\. `queries` Table

**Documentation 📜**
The `queries` table stores specific search or analysis queries related to products. This allows for tracking what queries are being run against product data and their current status.

| Column Name | Data Type | Nullable | Default Value | Description |
| :--- | :--- | :--- | :--- | :--- |
| `query_id` | `bigint` | `NO` | `nextval('queries_query_id_seq')` | **Primary Key.** Unique identifier for each query. |
| `product_id` | `bigint` | `YES` | | **Foreign Key** referencing `products.product_id`. Links the query to a specific product. |
| `query_text` | `text` | `YES` | | The actual text of the query. |
| `query_type` | `character varying` | `YES` | | The category or type of the query (e.g., 'keyword', 'attribute\_extraction'). |
| `is_active` | `boolean` | `YES` | `true` | Boolean indicating if the query is currently active. |

**Table Creation 🛠️**

```sql
CREATE SEQUENCE IF NOT EXISTS queries_query_id_seq;

CREATE TABLE queries (
    query_id BIGINT PRIMARY KEY DEFAULT nextval('queries_query_id_seq'),
    product_id BIGINT,
    query_text TEXT,
    query_type CHARACTER VARYING,
    is_active BOOLEAN DEFAULT TRUE,
    CONSTRAINT fk_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id)
        ON DELETE CASCADE
);
```

**Indexing Commands ⚡**

```sql
-- Index on product_id for efficient foreign key lookups and joins with products
CREATE INDEX idx_queries_product_id ON queries (product_id);

-- Index on query_type for filtering queries by their category
CREATE INDEX idx_queries_query_type ON queries (query_type);

-- Index on is_active for quickly finding active/inactive queries
CREATE INDEX idx_queries_is_active ON queries (is_active);
```

-----

#### 4\. `llmtasks` Table

**Documentation 📜**
The `llmtasks` table tracks the status and results of tasks executed by Large Language Models (LLMs). These tasks are typically associated with specific scraping jobs and product queries, and store information about the LLM used, its output, and any errors.

| Column Name | Data Type | Nullable | Default Value | Description |
| :--- | :--- | :--- | :--- | :--- |
| `task_id` | `uuid` | `NO` | `gen_random_uuid()` | **Primary Key.** Unique identifier for each LLM task. |
| `job_id` | `uuid` | `YES` | | **Foreign Key** referencing `scrapejobs.job_id`. Links the LLM task to the initial scraping job. |
| `query_id` | `bigint` | `YES` | | **Foreign Key** referencing `queries.query_id`. Links the LLM task to a specific query. |
| `llm_model_name` | `character varying` | `YES` | | The name of the LLM model used for the task. |
| `status` | `character varying` | `YES` | | Current status of the LLM task (e.g., 'pending', 'running', 'completed', 'failed'). |
| `s3_output_path` | `text` | `YES` | | The S3 path where the LLM task's output is stored. |
| `error_message` | `text` | `YES` | | Detailed message if the LLM task failed. |
| `created_at` | `timestamp with time zone` | `YES` | `CURRENT_TIMESTAMP` | Timestamp when the LLM task was created. |
| `completed_at` | `timestamp with time zone` | `YES` | | Timestamp when the LLM task completed. |
| `product_name` | `text` | `YES` | | The name of the product that the LLM task is associated with. |
| `product_id` | `bigint` | `YES` | | **Foreign Key** referencing `products.product_id`. Links the LLM task to a specific product. |

**Table Creation 🛠️**

```sql
CREATE TABLE llmtasks (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID,
    query_id BIGINT,
    llm_model_name CHARACTER VARYING,
    status CHARACTER VARYING,
    s3_output_path TEXT,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    product_name TEXT,
    product_id BIGINT,
    CONSTRAINT fk_llmtasks_job
        FOREIGN KEY (job_id)
        REFERENCES scrapejobs (job_id)
        ON DELETE SET NULL,
    CONSTRAINT fk_llmtasks_query
        FOREIGN KEY (query_id)
        REFERENCES queries (query_id)
        ON DELETE SET NULL,
    CONSTRAINT fk_llmtasks_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id)
        ON DELETE SET NULL
);
```

**Indexing Commands ⚡**

```sql
-- Index on job_id for efficient foreign key lookups and joins with scrapejobs
CREATE INDEX idx_llmtasks_job_id ON llmtasks (job_id);

-- Index on query_id for efficient foreign key lookups and joins with queries
CREATE INDEX idx_llmtasks_query_id ON llmtasks (query_id);

-- Index on product_id for efficient foreign key lookups and joins with products
CREATE INDEX idx_llmtasks_product_id ON llmtasks (product_id);

-- Index on status for quickly finding LLM tasks by their current state
CREATE INDEX idx_llmtasks_status ON llmtasks (status);

-- Index on llm_model_name for filtering tasks by the LLM model used
CREATE INDEX idx_llmtasks_llm_model_name ON llmtasks (llm_model_name);

-- Index on created_at for chronological sorting and range queries
CREATE INDEX idx_llmtasks_created_at ON llmtasks (created_at);
```