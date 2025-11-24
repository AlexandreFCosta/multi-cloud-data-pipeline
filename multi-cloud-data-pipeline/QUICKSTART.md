# 🚀 Quick Start Guide

Get started with Multi-Cloud Data Pipeline Framework in 5 minutes!

## Prerequisites

- Python 3.8 or higher
- pip package manager
- (Optional) Access to Azure or GCP account

## Installation

### Option 1: Install from PyPI (when published)
```bash
pip install multi-cloud-data-pipeline
```

### Option 2: Install from source
```bash
git clone https://github.com/YOUR-USERNAME/multi-cloud-data-pipeline.git
cd multi-cloud-data-pipeline
pip install -r requirements.txt
pip install -e .
```

### Option 3: Using Docker
```bash
docker pull YOUR-USERNAME/multicloud-pipeline:latest
docker run -it YOUR-USERNAME/multicloud-pipeline:latest
```

## Your First Pipeline (5 minutes)

### 1. Basic Batch Pipeline

Create a file `my_first_pipeline.py`:

```python
from multicloud_pipeline import Pipeline
from multicloud_pipeline.connectors.azure_connectors import AzureConnector
from multicloud_pipeline.transformers.spark_transformers import SparkTransformer

# Initialize pipeline
pipeline = Pipeline(
    name="my_first_pipeline",
    cloud_provider="azure"
)

# Add source
source = AzureConnector(
    connection_type="blob_storage",
    container="data",
    path="sales/*.parquet"
)

# Add transformation
transformer = SparkTransformer(
    transformation_type="aggregate",
    group_by=["product_id"],
    aggregations={"revenue": "sum"}
)

# Build and run
pipeline.add_source(source)
pipeline.add_transformer(transformer)

# Validate before running
print(pipeline.dry_run())
```

### 2. Run Your Pipeline

```bash
python my_first_pipeline.py
```

## Common Use Cases

### Batch ETL Pipeline

```python
from multicloud_pipeline import Pipeline
from multicloud_pipeline.connectors.gcp_connectors import GCPConnector
from multicloud_pipeline.transformers.spark_transformers import DataCleaningTransformer

pipeline = Pipeline("daily_etl", cloud_provider="gcp")

# Read from Cloud Storage
source = GCPConnector(
    connection_type="cloud_storage",
    bucket="my-bucket",
    path="raw/*.parquet"
)

# Clean data
cleaning = DataCleaningTransformer(
    remove_nulls=["id", "date"],
    standardize_columns=True
)

# Write to BigQuery
sink = GCPConnector(
    connection_type="bigquery",
    project_id="my-project",
    dataset="analytics",
    table="processed_data"
)

pipeline.add_source(source).add_transformer(cleaning).add_sink(sink)
pipeline.run()
```

### Real-Time Streaming Pipeline

```python
from multicloud_pipeline import StreamingPipeline
from multicloud_pipeline.connectors.gcp_connectors import GCPPubSubConnector

stream = StreamingPipeline(
    name="realtime_events",
    cloud_provider="gcp",
    checkpoint_location="gs://checkpoints/"
)

# Read from Pub/Sub
source = GCPPubSubConnector(
    project_id="my-project",
    subscription="events-sub"
)

stream.add_source(source)
stream.start()
```

### Cross-Cloud Migration

```python
from multicloud_pipeline import CrossCloudPipeline

migration = CrossCloudPipeline(
    source_cloud="azure",
    target_cloud="gcp"
)

migration.migrate(
    source={
        "type": "azure_data_lake",
        "path": "abfss://data@storage.dfs.core.windows.net/"
    },
    target={
        "type": "bigquery",
        "dataset": "migrated_data"
    }
)
```

## Configuration

### Azure Setup

Create `config/azure.yaml`:

```yaml
cloud: azure
resources:
  storage:
    account_name: ${AZURE_STORAGE_ACCOUNT}
    container: data-lake
  databricks:
    workspace_url: ${DATABRICKS_WORKSPACE_URL}
```

Set environment variables:
```bash
export AZURE_STORAGE_ACCOUNT=mystorageaccount
export DATABRICKS_WORKSPACE_URL=https://adb-xxx.azuredatabricks.net
```

### GCP Setup

Create `config/gcp.yaml`:

```yaml
cloud: gcp
resources:
  project_id: ${GCP_PROJECT_ID}
  bigquery:
    dataset: analytics
  storage:
    bucket: my-data-lake
```

Set environment variables:
```bash
export GCP_PROJECT_ID=my-project-id
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

## Testing Your Setup

Run the included tests:

```bash
# Run all tests
pytest tests/

# Run specific test
pytest tests/test_pipeline.py
```

## Example Projects

Check the `examples/` directory for complete examples:

1. **Batch ETL**: `examples/batch_etl/sales_pipeline.py`
2. **Streaming**: `examples/streaming/realtime_events.py`
3. **Migration**: `examples/cross_cloud/azure_to_gcp.py`

## Next Steps

- 📚 Read the [full documentation](docs/)
- 🎯 Check out [advanced examples](examples/)
- 🏗️ Deploy infrastructure with [Terraform](terraform/)
- 🤝 Contribute to the project (see [CONTRIBUTING.md](CONTRIBUTING.md))

## Troubleshooting

### Common Issues

**Issue**: PySpark not found
```bash
# Solution: Install PySpark
pip install pyspark==3.5.0
```

**Issue**: Cloud authentication fails
```bash
# Azure: Login with Azure CLI
az login

# GCP: Set credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

**Issue**: Import errors
```bash
# Solution: Ensure package is installed
pip install -e .
```

## Getting Help

- 📖 [Documentation](docs/)
- 💬 [GitHub Discussions](https://github.com/YOUR-USERNAME/multi-cloud-data-pipeline/discussions)
- 🐛 [Issue Tracker](https://github.com/YOUR-USERNAME/multi-cloud-data-pipeline/issues)
- 📧 Email: your.email@example.com

## Resources

- [Azure Databricks Documentation](https://docs.microsoft.com/azure/databricks/)
- [Google BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Terraform Documentation](https://www.terraform.io/docs)

Happy data engineering! 🎉
