"""
Example: Real-Time Streaming Pipeline
Process user events from Kafka/Pub/Sub and write to BigQuery
"""

import sys
sys.path.append('../../src')

from multicloud_pipeline import StreamingPipeline
from multicloud_pipeline.connectors.gcp_connectors import GCPPubSubConnector, GCPConnector
from multicloud_pipeline.transformers.spark_transformers import SparkTransformer, SQLTransformer


def create_realtime_events_pipeline():
    """
    Create a real-time streaming pipeline for user events.
    
    Pipeline flow:
    1. Read events from GCP Pub/Sub
    2. Parse JSON messages
    3. Apply windowed aggregations (5-minute windows)
    4. Write to BigQuery for real-time analytics
    """
    
    # Initialize streaming pipeline
    pipeline = StreamingPipeline(
        name="realtime_user_events",
        cloud_provider="gcp",
        checkpoint_location="gs://my-bucket/checkpoints/user-events",
        trigger="processingTime='1 minute'"
    )
    
    # Step 1: Configure Pub/Sub source
    source = GCPPubSubConnector(
        project_id="my-project",
        subscription="user-events-sub",
        starting_position="latest"
    )
    
    # Step 2: Parse JSON and extract fields
    parse_json = SQLTransformer(
        sql_query="""
            SELECT
                get_json_object(CAST(data AS STRING), '$.user_id') as user_id,
                get_json_object(CAST(data AS STRING), '$.event_type') as event_type,
                get_json_object(CAST(data AS STRING), '$.timestamp') as event_timestamp,
                get_json_object(CAST(data AS STRING), '$.properties') as properties,
                CAST(publishTime as TIMESTAMP) as publish_time
            FROM temp_table
        """,
        temp_view_name="temp_table"
    )
    
    # Step 3: Windowed aggregation (5-minute tumbling windows)
    aggregation = SparkTransformer(
        transformation_type="window",
        partition_by=["user_id", "event_type"],
        order_by=["event_timestamp"],
        window_function="count"
    )
    
    # Step 4: Configure BigQuery sink
    sink = GCPConnector(
        connection_type="bigquery",
        project_id="my-project",
        dataset="analytics",
        table="realtime_events",
        temp_bucket="my-temp-bucket",
        mode="append"
    )
    
    # Build pipeline
    pipeline.add_source(source)
    pipeline.add_transformer(parse_json)
    pipeline.add_transformer(aggregation)
    pipeline.add_sink(sink)
    
    return pipeline


def main():
    """Execute the streaming pipeline"""
    
    print("=" * 80)
    print("Starting Real-Time User Events Pipeline")
    print("=" * 80)
    
    # Create pipeline
    pipeline = create_realtime_events_pipeline()
    
    # Validate pipeline
    print("\n>>> Validating pipeline configuration...")
    validation = pipeline.dry_run()
    print(f"Validation results: {validation}")
    
    if not validation['valid']:
        print("❌ Pipeline validation failed!")
        return
    
    print("✅ Pipeline validation passed!")
    
    # Start streaming
    print("\n>>> Starting streaming pipeline...")
    try:
        result = pipeline.start()
        
        print("\n" + "=" * 80)
        print("Streaming Pipeline Status")
        print("=" * 80)
        print(f"Status: {result['status']}")
        print(f"Pipeline: {result['pipeline']}")
        print(f"Trigger: {result['trigger']}")
        
        print("\n✅ Streaming pipeline is now running!")
        print("Press Ctrl+C to stop...")
        
        # In a real application, this would keep running
        # For demo purposes, we'll just show the status
        
    except KeyboardInterrupt:
        print("\n\n>>> Stopping streaming pipeline...")
        pipeline.stop()
        print("✅ Pipeline stopped successfully!")
        
    except Exception as e:
        print(f"\n❌ Pipeline execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
