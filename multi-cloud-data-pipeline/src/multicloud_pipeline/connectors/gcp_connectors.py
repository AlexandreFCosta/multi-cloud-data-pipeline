"""
GCP-specific data connectors for Multi-Cloud Pipeline Framework
Supports BigQuery, Cloud Storage, Pub/Sub, and Cloud SQL
"""

from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import logging


@dataclass
class GCPConfig:
    """Configuration for GCP resources"""
    project_id: str
    region: Optional[str] = "us-central1"
    credentials_path: Optional[str] = None
    use_service_account: bool = True


class GCPConnector:
    """
    Base connector for GCP data sources.
    
    Example:
        >>> connector = GCPConnector(
        ...     connection_type="bigquery",
        ...     dataset="analytics",
        ...     table="sales"
        ... )
    """
    
    def __init__(
        self,
        connection_type: str,
        project_id: Optional[str] = None,
        dataset: Optional[str] = None,
        table: Optional[str] = None,
        bucket: Optional[str] = None,
        path: Optional[str] = None,
        config: Optional[GCPConfig] = None,
        **kwargs
    ):
        """
        Initialize GCP connector.
        
        Args:
            connection_type: Type of connection (bigquery, cloud_storage, cloud_sql)
            project_id: GCP project ID
            dataset: BigQuery dataset name
            table: Table name
            bucket: Cloud Storage bucket name
            path: Path within bucket
            config: GCP configuration object
        """
        self.connection_type = connection_type
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.bucket = bucket
        self.path = path
        self.config = config or self._get_default_config()
        self.options = kwargs
        
        self.logger = logging.getLogger(f"gcp.{connection_type}")
        self.logger.info(f"GCP connector initialized: {connection_type}")
    
    def _get_default_config(self) -> GCPConfig:
        """Get default GCP configuration"""
        return GCPConfig(
            project_id=self.project_id or "default-project",
            region="us-central1"
        )
    
    def read(self, spark) -> Any:
        """
        Read data from GCP source.
        
        Args:
            spark: Spark session
            
        Returns:
            Spark DataFrame
        """
        self.logger.info(f"Reading from GCP {self.connection_type}")
        
        if self.connection_type == "bigquery":
            return self._read_bigquery(spark)
        elif self.connection_type == "cloud_storage":
            return self._read_cloud_storage(spark)
        elif self.connection_type == "cloud_sql":
            return self._read_cloud_sql(spark)
        else:
            raise ValueError(f"Unsupported connection type: {self.connection_type}")
    
    def _read_bigquery(self, spark):
        """Read from BigQuery"""
        self.logger.info(f"Reading from BigQuery: {self.project_id}.{self.dataset}.{self.table}")
        
        # Configure BigQuery connector
        table_ref = f"{self.project_id}:{self.dataset}.{self.table}" if self.table else None
        query = self.options.get('query')
        
        reader = spark.read.format("bigquery")
        
        if table_ref:
            reader = reader.option("table", table_ref)
        elif query:
            reader = reader.option("query", query)
        else:
            raise ValueError("Either table or query must be specified for BigQuery")
        
        # Add optional configurations
        if self.config.credentials_path:
            reader = reader.option("credentialsFile", self.config.credentials_path)
        
        # Enable optimizations
        reader = (reader
                  .option("viewsEnabled", "true")
                  .option("materializationDataset", self.options.get('temp_dataset', self.dataset)))
        
        df = reader.load()
        
        self.logger.info("Successfully read data from BigQuery")
        return df
    
    def _read_cloud_storage(self, spark):
        """Read from Cloud Storage"""
        self.logger.info(f"Reading from Cloud Storage: gs://{self.bucket}/{self.path}")
        
        gcs_path = f"gs://{self.bucket}/{self.path}"
        
        # Configure GCS access
        if self.config.credentials_path:
            spark.conf.set(
                "google.cloud.auth.service.account.json.keyfile",
                self.config.credentials_path
            )
        
        file_format = self.options.get('format', 'parquet')
        
        df = spark.read.format(file_format).load(gcs_path)
        
        self.logger.info("Successfully read data from Cloud Storage")
        return df
    
    def _read_cloud_sql(self, spark):
        """Read from Cloud SQL"""
        self.logger.info(f"Reading from Cloud SQL: {self.table}")
        
        jdbc_url = self.options.get('jdbc_url')
        
        if not jdbc_url:
            instance = self.options.get('instance')
            database = self.options.get('database')
            jdbc_url = f"jdbc:postgresql:///{database}?cloudSqlInstance={instance}&socketFactory=com.google.cloud.sql.postgres.SocketFactory"
        
        reader = (spark.read
                  .format("jdbc")
                  .option("url", jdbc_url)
                  .option("dbtable", self.table)
                  .option("driver", "org.postgresql.Driver"))
        
        if 'user' in self.options:
            reader = reader.option("user", self.options['user'])
        if 'password' in self.options:
            reader = reader.option("password", self.options['password'])
        
        df = reader.load()
        
        self.logger.info("Successfully read data from Cloud SQL")
        return df
    
    def write(self, df) -> Dict[str, Any]:
        """
        Write data to GCP destination.
        
        Args:
            df: Spark DataFrame to write
            
        Returns:
            Write operation results
        """
        self.logger.info(f"Writing to GCP {self.connection_type}")
        
        if self.connection_type == "bigquery":
            return self._write_bigquery(df)
        elif self.connection_type == "cloud_storage":
            return self._write_cloud_storage(df)
        elif self.connection_type == "cloud_sql":
            return self._write_cloud_sql(df)
        else:
            raise ValueError(f"Unsupported connection type: {self.connection_type}")
    
    def _write_bigquery(self, df) -> Dict[str, Any]:
        """Write to BigQuery"""
        self.logger.info(f"Writing to BigQuery: {self.project_id}.{self.dataset}.{self.table}")
        
        table_ref = f"{self.project_id}:{self.dataset}.{self.table}"
        mode = self.options.get('mode', 'overwrite')
        
        writer = (df.write
                  .format("bigquery")
                  .option("table", table_ref)
                  .mode(mode))
        
        # Add optional configurations
        if self.config.credentials_path:
            writer = writer.option("credentialsFile", self.config.credentials_path)
        
        # Set temp bucket for staging
        temp_bucket = self.options.get('temp_bucket', self.bucket)
        if temp_bucket:
            writer = writer.option("temporaryGcsBucket", temp_bucket)
        
        # Partitioning support
        if 'partition_field' in self.options:
            writer = writer.option("partitionField", self.options['partition_field'])
            writer = writer.option("partitionType", self.options.get('partition_type', 'DAY'))
        
        # Clustering support
        if 'clustering_fields' in self.options:
            writer = writer.option("clusteredFields", self.options['clustering_fields'])
        
        writer.save()
        
        self.logger.info("Successfully wrote data to BigQuery")
        
        return {
            "status": "success",
            "destination": table_ref,
            "mode": mode
        }
    
    def _write_cloud_storage(self, df) -> Dict[str, Any]:
        """Write to Cloud Storage"""
        self.logger.info(f"Writing to Cloud Storage: gs://{self.bucket}/{self.path}")
        
        gcs_path = f"gs://{self.bucket}/{self.path}"
        file_format = self.options.get('format', 'parquet')
        mode = self.options.get('mode', 'overwrite')
        
        writer = df.write.format(file_format).mode(mode)
        
        # Add partition columns if specified
        if 'partition_by' in self.options:
            writer = writer.partitionBy(self.options['partition_by'])
        
        writer.save(gcs_path)
        
        self.logger.info("Successfully wrote data to Cloud Storage")
        
        return {
            "status": "success",
            "destination": gcs_path,
            "format": file_format,
            "mode": mode
        }
    
    def _write_cloud_sql(self, df) -> Dict[str, Any]:
        """Write to Cloud SQL"""
        self.logger.info(f"Writing to Cloud SQL: {self.table}")
        
        jdbc_url = self.options.get('jdbc_url')
        mode = self.options.get('mode', 'overwrite')
        
        if not jdbc_url:
            instance = self.options.get('instance')
            database = self.options.get('database')
            jdbc_url = f"jdbc:postgresql:///{database}?cloudSqlInstance={instance}&socketFactory=com.google.cloud.sql.postgres.SocketFactory"
        
        writer = (df.write
                  .format("jdbc")
                  .option("url", jdbc_url)
                  .option("dbtable", self.table)
                  .option("driver", "org.postgresql.Driver")
                  .mode(mode))
        
        if 'user' in self.options:
            writer = writer.option("user", self.options['user'])
        if 'password' in self.options:
            writer = writer.option("password", self.options['password'])
        
        writer.save()
        
        self.logger.info("Successfully wrote data to Cloud SQL")
        
        return {
            "status": "success",
            "destination": f"cloudsql://{self.table}",
            "mode": mode
        }


class GCPPubSubConnector:
    """
    Connector for GCP Pub/Sub streaming data.
    
    Example:
        >>> connector = GCPPubSubConnector(
        ...     project_id="my-project",
        ...     topic="events",
        ...     subscription="events-sub"
        ... )
    """
    
    def __init__(
        self,
        project_id: str,
        topic: Optional[str] = None,
        subscription: Optional[str] = None,
        credentials_path: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Pub/Sub connector.
        
        Args:
            project_id: GCP project ID
            topic: Pub/Sub topic name (for writing)
            subscription: Pub/Sub subscription name (for reading)
            credentials_path: Path to service account credentials
        """
        self.project_id = project_id
        self.topic = topic
        self.subscription = subscription
        self.credentials_path = credentials_path
        self.options = kwargs
        
        self.logger = logging.getLogger("gcp.pubsub")
        self.logger.info(f"Pub/Sub connector initialized: {project_id}")
    
    def read_stream(self, spark):
        """Read streaming data from Pub/Sub"""
        if not self.subscription:
            raise ValueError("Subscription is required for reading from Pub/Sub")
        
        self.logger.info(f"Reading stream from Pub/Sub: {self.subscription}")
        
        subscription_path = f"projects/{self.project_id}/subscriptions/{self.subscription}"
        
        reader = (spark.readStream
                  .format("pubsub")
                  .option("projectId", self.project_id)
                  .option("subscriptionId", self.subscription))
        
        if self.credentials_path:
            reader = reader.option("credentialsFile", self.credentials_path)
        
        df = reader.load()
        
        self.logger.info("Stream reading initiated from Pub/Sub")
        return df
    
    def write_stream(self, df, checkpoint_location: str):
        """Write streaming data to Pub/Sub"""
        if not self.topic:
            raise ValueError("Topic is required for writing to Pub/Sub")
        
        self.logger.info(f"Writing stream to Pub/Sub: {self.topic}")
        
        topic_path = f"projects/{self.project_id}/topics/{self.topic}"
        
        writer = (df.writeStream
                  .format("pubsub")
                  .option("projectId", self.project_id)
                  .option("topicId", self.topic)
                  .option("checkpointLocation", checkpoint_location))
        
        if self.credentials_path:
            writer = writer.option("credentialsFile", self.credentials_path)
        
        query = writer.start()
        
        self.logger.info("Stream writing initiated to Pub/Sub")
        return query


class GCPDataflowConnector:
    """
    Connector for GCP Dataflow batch and streaming jobs.
    
    Example:
        >>> connector = GCPDataflowConnector(
        ...     project_id="my-project",
        ...     region="us-central1",
        ...     temp_location="gs://temp-bucket/"
        ... )
    """
    
    def __init__(
        self,
        project_id: str,
        region: str = "us-central1",
        temp_location: Optional[str] = None,
        staging_location: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Dataflow connector.
        
        Args:
            project_id: GCP project ID
            region: GCP region for Dataflow jobs
            temp_location: GCS path for temporary files
            staging_location: GCS path for staging files
        """
        self.project_id = project_id
        self.region = region
        self.temp_location = temp_location
        self.staging_location = staging_location
        self.options = kwargs
        
        self.logger = logging.getLogger("gcp.dataflow")
        self.logger.info(f"Dataflow connector initialized: {project_id}/{region}")
    
    def get_pipeline_options(self) -> Dict[str, Any]:
        """Get Dataflow pipeline options"""
        options = {
            'project': self.project_id,
            'region': self.region,
            'runner': 'DataflowRunner',
            'temp_location': self.temp_location,
            'staging_location': self.staging_location or self.temp_location,
        }
        
        # Add additional options
        options.update(self.options)
        
        return options
