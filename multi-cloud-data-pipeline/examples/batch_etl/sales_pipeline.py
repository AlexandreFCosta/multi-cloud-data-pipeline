"""
Example: Batch ETL Pipeline
Daily sales data processing from Azure Blob Storage to Synapse Analytics
"""

import sys
sys.path.append('../../src')

from multicloud_pipeline import Pipeline
from multicloud_pipeline.connectors.azure_connectors import AzureConnector
from multicloud_pipeline.transformers.spark_transformers import (
    SparkTransformer,
    DataCleaningTransformer,
    FeatureEngineeringTransformer
)


def create_sales_etl_pipeline():
    """
    Create a complete ETL pipeline for daily sales data.
    
    Pipeline flow:
    1. Read sales data from Azure Blob Storage (Parquet format)
    2. Clean and standardize data
    3. Extract date features
    4. Aggregate by product and date
    5. Write results to Azure Synapse Analytics
    """
    
    # Initialize pipeline
    pipeline = Pipeline(
        name="daily_sales_etl",
        cloud_provider="azure",
        enable_monitoring=True,
        enable_lineage=True
    )
    
    # Step 1: Configure source - Azure Blob Storage
    source = AzureConnector(
        connection_type="blob_storage",
        container="raw-data",
        path="sales/year=2024/month=11/*.parquet",
        format="parquet"
    )
    
    # Step 2: Data cleaning transformation
    cleaning = DataCleaningTransformer(
        remove_nulls=["transaction_id", "product_id", "customer_id"],
        standardize_columns=True,
        trim_strings=True,
        remove_duplicates=True
    )
    
    # Step 3: Feature engineering - extract date features
    feature_engineering = FeatureEngineeringTransformer(
        date_features=["transaction_date"],
        binning={
            "amount": [0, 50, 100, 500, 1000, float('inf')]
        }
    )
    
    # Step 4: Aggregation transformation
    aggregation = SparkTransformer(
        transformation_type="aggregate",
        group_by=["product_id", "transaction_date"],
        aggregations={
            "amount": "sum",
            "quantity": "sum",
            "transaction_id": "count_distinct",
            "customer_id": "count_distinct"
        }
    )
    
    # Step 5: Configure sink - Azure Synapse
    sink = AzureConnector(
        connection_type="synapse",
        container="staging",
        jdbc_url="jdbc:sqlserver://mysynapse.sql.azuresynapse.net:1433;database=analytics",
        table="sales.daily_aggregates",
        mode="append"
    )
    
    # Build pipeline
    pipeline.add_source(source)
    pipeline.add_transformer(cleaning)
    pipeline.add_transformer(feature_engineering)
    pipeline.add_transformer(aggregation)
    pipeline.add_sink(sink)
    
    return pipeline


def main():
    """Execute the sales ETL pipeline"""
    
    print("=" * 80)
    print("Starting Daily Sales ETL Pipeline")
    print("=" * 80)
    
    # Create pipeline
    pipeline = create_sales_etl_pipeline()
    
    # Perform dry run first
    print("\n>>> Performing dry run validation...")
    validation = pipeline.dry_run()
    print(f"Validation results: {validation}")
    
    if not validation['valid']:
        print("❌ Pipeline validation failed!")
        return
    
    print("✅ Pipeline validation passed!")
    
    # Execute pipeline
    print("\n>>> Executing pipeline...")
    try:
        results = pipeline.run()
        
        print("\n" + "=" * 80)
        print("Pipeline Execution Summary")
        print("=" * 80)
        print(f"Status: {results['status']}")
        print(f"Duration: {results['duration_seconds']:.2f} seconds")
        print(f"Records Processed: {results['records_processed']:,}")
        print(f"Start Time: {results['start_time']}")
        print(f"End Time: {results['end_time']}")
        
        print("\n✅ Pipeline completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Pipeline execution failed: {str(e)}")
        raise
    
    # Display data lineage
    print("\n>>> Data Lineage:")
    lineage = pipeline.get_lineage()
    print(f"Pipeline: {lineage['pipeline']}")
    print(f"Sources: {', '.join(lineage['sources'])}")
    print(f"Transformations: {', '.join(lineage['transformations'])}")
    print(f"Sinks: {', '.join(lineage['sinks'])}")


if __name__ == "__main__":
    main()
