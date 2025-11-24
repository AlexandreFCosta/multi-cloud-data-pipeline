"""
Google Cloud Platform Connectors

Connectors for GCP services including BigQuery, Cloud Storage,
Pub/Sub, Cloud SQL, and Dataflow.
"""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)


class GCPConnector:
    """Base class for GCP connectors"""
    
    def __init__(
        self,
        spark: SparkSession,
        project_id: str,
        config: Optional[Dict[str, Any]] = None
    ):
        self.spark = spark
        self.project_id = project_id
        self.config = config or {}
        
        # Configure GCP credentials if provided
        if "credentials_path" in config:
            self.spark.conf.set(
                "google.cloud.auth.service.account.json.keyfile",
                config["credentials_path"]
            )


class GCPBigQueryConnector(GCPConnector):
    """
    Connector for Google BigQuery
    
    Example:
        >>> connector = GCPBigQueryConnector(
        ...     spark=spark,
        ...     project_id="my-project",
        ...     dataset="analytics"
        ... )
        >>> df = connector.read(table="sales")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        project_id: str,
        dataset: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(spark, project_id, config)
        self.dataset = dataset
        
        # Configure BigQuery connector
        self.spark.conf.set("temporaryGcsBucket", config.get("temp_bucket", f"{project_id}-temp"))
    
    def read(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Read data from BigQuery
        
        Args:
            table: Table name (can be dataset.table or just table if dataset is set)
            query: SQL query to execute (use table or query, not both)
            options: Additional BigQuery options
            
        Returns:
            Spark DataFrame
        """
        if table and query:
            raise ValueError("Provide either table or query, not both")
        
        if not table and not query:
            raise ValueError("Provide either table or query")
        
        reader = self.spark.read.format("bigquery")
        
        if table:
            # Build full table reference
            if "." not in table and self.dataset:
                full_table = f"{self.project_id}.{self.dataset}.{table}"
            elif "." not in table:
                raise ValueError("Dataset must be specified either in constructor or table name")
            else:
                full_table = f"{self.project_id}.{table}"
            
            logger.info(f"Reading from BigQuery table: {full_table}")
            reader = reader.option("table", full_table)
        else:
            logger.info(f"Reading from BigQuery query")
            reader = reader.option("query", query)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        return reader.load()
    
    def write(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        partition_field: Optional[str] = None,
        clustering_fields: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None
    ):
        """
        Write data to BigQuery
        
        Args:
            df: Spark DataFrame to write
            table: Target table name (dataset.table or just table)
            mode: Write mode (overwrite, append, ignore, error)
            partition_field: Field to partition by (DATE or TIMESTAMP column)
            clustering_fields: List of fields to cluster by
            options: Additional BigQuery options
        """
        # Build full table reference
        if "." not in table and self.dataset:
            full_table = f"{self.project_id}.{self.dataset}.{table}"
        elif "." not in table:
            raise ValueError("Dataset must be specified either in constructor or table name")
        else:
            full_table = f"{self.project_id}.{table}"
        
        logger.info(f"Writing to BigQuery table: {full_table}")
        
        writer = df.write.format("bigquery") \
            .option("table", full_table) \
            .mode(mode)
        
        if partition_field:
            writer = writer.option("partitionField", partition_field)
            writer = writer.option("partitionType", "DAY")
        
        if clustering_fields:
            writer = writer.option("clusteredFields", ",".join(clustering_fields))
        
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)
        
        writer.save()


class GCPCloudStorageConnector(GCPConnector):
    """
    Connector for Google Cloud Storage
    
    Example:
        >>> connector = GCPCloudStorageConnector(
        ...     spark=spark,
        ...     project_id="my-project",
        ...     bucket="my-data-lake"
        ... )
        >>> df = connector.read(path="raw/sales/*.parquet")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        project_id: str,
        bucket: str,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(spark, project_id, config)
        self.bucket = bucket
    
    def read(
        self,
        path: str,
        format: str = "parquet",
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Read data from Cloud Storage
        
        Args:
            path: Path within the bucket
            format: File format (parquet, csv, json, avro)
            options: Additional Spark read options
            
        Returns:
            Spark DataFrame
        """
        full_path = f"gs://{self.bucket}/{path}"
        
        logger.info(f"Reading from GCS: {full_path}")
        
        reader = self.spark.read.format(format)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        return reader.load(full_path)
    
    def write(
        self,
        df: DataFrame,
        path: str,
        format: str = "parquet",
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None
    ):
        """
        Write data to Cloud Storage
        
        Args:
            df: Spark DataFrame to write
            path: Target path within the bucket
            format: File format (parquet, csv, json, avro)
            mode: Write mode (overwrite, append, ignore, error)
            partition_by: List of columns to partition by
            options: Additional Spark write options
        """
        full_path = f"gs://{self.bucket}/{path}"
        
        logger.info(f"Writing to GCS: {full_path}")
        
        writer = df.write.format(format).mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)
        
        writer.save(full_path)


class GCPPubSubConnector:
    """
    Connector for Google Cloud Pub/Sub (streaming)
    
    Example:
        >>> connector = GCPPubSubConnector(
        ...     spark=spark,
        ...     project_id="my-project",
        ...     subscription="my-subscription"
        ... )
        >>> df = connector.read_stream()
    """
    
    def __init__(
        self,
        spark: SparkSession,
        project_id: str,
        subscription: Optional[str] = None,
        topic: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        self.spark = spark
        self.project_id = project_id
        self.subscription = subscription
        self.topic = topic
        self.config = config or {}
        
        if not subscription and not topic:
            raise ValueError("Either subscription or topic must be provided")
    
    def read_stream(
        self,
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Read streaming data from Pub/Sub
        
        Args:
            options: Additional Pub/Sub options
            
        Returns:
            Streaming DataFrame
        """
        if self.subscription:
            subscription_path = f"projects/{self.project_id}/subscriptions/{self.subscription}"
            logger.info(f"Reading stream from Pub/Sub subscription: {subscription_path}")
        else:
            topic_path = f"projects/{self.project_id}/topics/{self.topic}"
            logger.info(f"Reading stream from Pub/Sub topic: {topic_path}")
        
        reader = self.spark.readStream.format("pubsub")
        
        if self.subscription:
            reader = reader.option("subscription", subscription_path)
        else:
            reader = reader.option("topic", topic_path)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        return reader.load()
    
    def write_stream(
        self,
        df: DataFrame,
        checkpoint_location: str,
        output_mode: str = "append",
        trigger: str = "processingTime='10 seconds'"
    ):
        """
        Write streaming data to Pub/Sub
        
        Args:
            df: Streaming DataFrame to write
            checkpoint_location: Checkpoint location for fault tolerance
            output_mode: Output mode (append, update, complete)
            trigger: Trigger interval
        """
        if not self.topic:
            raise ValueError("Topic must be specified for writing")
        
        topic_path = f"projects/{self.project_id}/topics/{self.topic}"
        logger.info(f"Writing stream to Pub/Sub topic: {topic_path}")
        
        query = df.writeStream \
            .format("pubsub") \
            .outputMode(output_mode) \
            .option("topic", topic_path) \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(processingTime=trigger) \
            .start()
        
        return query


class GCPCloudSQLConnector:
    """
    Connector for Google Cloud SQL
    
    Example:
        >>> connector = GCPCloudSQLConnector(
        ...     spark=spark,
        ...     project_id="my-project",
        ...     instance="my-instance",
        ...     database="salesdb",
        ...     config={"user": "admin", "password": "..."}
        ... )
        >>> df = connector.read(table="customers")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        project_id: str,
        region: str,
        instance: str,
        database: str,
        db_type: str = "postgres",  # postgres or mysql
        config: Optional[Dict[str, Any]] = None
    ):
        self.spark = spark
        self.project_id = project_id
        self.region = region
        self.instance = instance
        self.database = database
        self.db_type = db_type
        self.config = config or {}
        
        # Build connection string
        if db_type == "postgres":
            self.jdbc_url = f"jdbc:postgresql:///{database}?cloudSqlInstance={project_id}:{region}:{instance}&socketFactory=com.google.cloud.sql.postgres.SocketFactory"
            self.driver = "org.postgresql.Driver"
        else:  # mysql
            self.jdbc_url = f"jdbc:mysql:///{database}?cloudSqlInstance={project_id}:{region}:{instance}&socketFactory=com.google.cloud.sql.mysql.SocketFactory"
            self.driver = "com.mysql.cj.jdbc.Driver"
        
        self.connection_properties = {
            "user": config.get("user", ""),
            "password": config.get("password", ""),
            "driver": self.driver
        }
    
    def read(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Read data from Cloud SQL
        
        Args:
            table: Table name to read
            query: SQL query to execute
            options: Additional JDBC options
            
        Returns:
            Spark DataFrame
        """
        if table and query:
            raise ValueError("Provide either table or query, not both")
        
        logger.info(f"Reading from Cloud SQL: {table or 'custom query'}")
        
        reader = self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url)
        
        for key, value in self.connection_properties.items():
            reader = reader.option(key, value)
        
        if table:
            reader = reader.option("dbtable", table)
        else:
            reader = reader.option("query", query)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        return reader.load()
    
    def write(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        options: Optional[Dict[str, str]] = None
    ):
        """
        Write data to Cloud SQL
        
        Args:
            df: Spark DataFrame to write
            table: Target table name
            mode: Write mode (overwrite, append, ignore, error)
            options: Additional JDBC options
        """
        logger.info(f"Writing to Cloud SQL table: {table}")
        
        writer = df.write.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table) \
            .mode(mode)
        
        for key, value in self.connection_properties.items():
            writer = writer.option(key, value)
        
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)
        
        writer.save()


class GCPDataflowConnector:
    """
    Helper class for Dataflow configurations
    
    Example:
        >>> config = GCPDataflowConnector.get_config(
        ...     project_id="my-project",
        ...     region="us-central1",
        ...     temp_location="gs://my-bucket/temp"
        ... )
    """
    
    @staticmethod
    def get_config(
        project_id: str,
        region: str,
        temp_location: str,
        staging_location: Optional[str] = None,
        additional_options: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """
        Get Dataflow configuration for Spark
        
        Args:
            project_id: GCP project ID
            region: GCP region
            temp_location: GCS path for temporary files
            staging_location: GCS path for staging files
            additional_options: Additional Dataflow options
            
        Returns:
            Dictionary of Dataflow configuration options
        """
        config = {
            "spark.dataflow.project.id": project_id,
            "spark.dataflow.region": region,
            "spark.dataflow.tempLocation": temp_location,
            "spark.dataflow.stagingLocation": staging_location or f"{temp_location}/staging"
        }
        
        if additional_options:
            config.update(additional_options)
        
        return config


# Export all connectors
__all__ = [
    "GCPConnector",
    "GCPBigQueryConnector",
    "GCPCloudStorageConnector",
    "GCPPubSubConnector",
    "GCPCloudSQLConnector",
    "GCPDataflowConnector"
]
