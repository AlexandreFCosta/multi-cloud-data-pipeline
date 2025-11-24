"""
Azure-specific data connectors for Multi-Cloud Pipeline Framework
Supports Azure Data Lake, Blob Storage, Synapse, and Event Hubs
"""

from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import logging


@dataclass
class AzureConfig:
    """Configuration for Azure resources"""
    storage_account: str
    container: Optional[str] = None
    access_key: Optional[str] = None
    sas_token: Optional[str] = None
    use_managed_identity: bool = True


class AzureConnector:
    """
    Base connector for Azure data sources.
    
    Example:
        >>> connector = AzureConnector(
        ...     connection_type="blob_storage",
        ...     container="raw-data",
        ...     path="sales/*.parquet"
        ... )
    """
    
    def __init__(
        self,
        connection_type: str,
        container: Optional[str] = None,
        path: Optional[str] = None,
        config: Optional[AzureConfig] = None,
        **kwargs
    ):
        """
        Initialize Azure connector.
        
        Args:
            connection_type: Type of connection (blob_storage, data_lake, synapse)
            container: Container or filesystem name
            path: Path to data within container
            config: Azure configuration object
        """
        self.connection_type = connection_type
        self.container = container
        self.path = path
        self.config = config or self._get_default_config()
        self.options = kwargs
        
        self.logger = logging.getLogger(f"azure.{connection_type}")
        self.logger.info(f"Azure connector initialized: {connection_type}")
    
    def _get_default_config(self) -> AzureConfig:
        """Get default Azure configuration"""
        return AzureConfig(
            storage_account="default_account",
            use_managed_identity=True
        )
    
    def read(self, spark) -> Any:
        """
        Read data from Azure source.
        
        Args:
            spark: Spark session
            
        Returns:
            Spark DataFrame
        """
        self.logger.info(f"Reading from Azure {self.connection_type}")
        
        if self.connection_type == "blob_storage":
            return self._read_blob_storage(spark)
        elif self.connection_type == "data_lake":
            return self._read_data_lake(spark)
        elif self.connection_type == "synapse":
            return self._read_synapse(spark)
        else:
            raise ValueError(f"Unsupported connection type: {self.connection_type}")
    
    def _read_blob_storage(self, spark):
        """Read from Azure Blob Storage"""
        self.logger.info(f"Reading from blob: {self.container}/{self.path}")
        
        # Configure Azure blob storage access
        if self.config.use_managed_identity:
            spark.conf.set(
                f"fs.azure.account.auth.type.{self.config.storage_account}.dfs.core.windows.net",
                "OAuth"
            )
            spark.conf.set(
                f"fs.azure.account.oauth.provider.type.{self.config.storage_account}.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
            )
        
        # Construct ABFS path
        abfs_path = f"abfss://{self.container}@{self.config.storage_account}.dfs.core.windows.net/{self.path}"
        
        # Read data (format inferred from path or options)
        file_format = self.options.get('format', 'parquet')
        
        df = spark.read.format(file_format).load(abfs_path)
        
        self.logger.info(f"Successfully read data from blob storage")
        return df
    
    def _read_data_lake(self, spark):
        """Read from Azure Data Lake Gen2"""
        self.logger.info(f"Reading from Data Lake: {self.container}/{self.path}")
        
        # Data Lake Gen2 uses ABFS protocol
        return self._read_blob_storage(spark)
    
    def _read_synapse(self, spark):
        """Read from Azure Synapse Analytics"""
        self.logger.info("Reading from Synapse Analytics")
        
        jdbc_url = self.options.get('jdbc_url')
        table = self.options.get('table')
        
        if not jdbc_url or not table:
            raise ValueError("jdbc_url and table are required for Synapse connection")
        
        df = (spark.read
              .format("com.databricks.spark.sqldw")
              .option("url", jdbc_url)
              .option("tempDir", f"abfss://{self.container}@{self.config.storage_account}.dfs.core.windows.net/temp")
              .option("forwardSparkAzureStorageCredentials", "true")
              .option("dbTable", table)
              .load())
        
        self.logger.info(f"Successfully read data from Synapse table: {table}")
        return df
    
    def write(self, df) -> Dict[str, Any]:
        """
        Write data to Azure destination.
        
        Args:
            df: Spark DataFrame to write
            
        Returns:
            Write operation results
        """
        self.logger.info(f"Writing to Azure {self.connection_type}")
        
        if self.connection_type == "blob_storage":
            return self._write_blob_storage(df)
        elif self.connection_type == "data_lake":
            return self._write_data_lake(df)
        elif self.connection_type == "synapse":
            return self._write_synapse(df)
        else:
            raise ValueError(f"Unsupported connection type: {self.connection_type}")
    
    def _write_blob_storage(self, df) -> Dict[str, Any]:
        """Write to Azure Blob Storage"""
        self.logger.info(f"Writing to blob: {self.container}/{self.path}")
        
        abfs_path = f"abfss://{self.container}@{self.config.storage_account}.dfs.core.windows.net/{self.path}"
        
        file_format = self.options.get('format', 'parquet')
        mode = self.options.get('mode', 'overwrite')
        
        writer = df.write.format(file_format).mode(mode)
        
        # Add partition columns if specified
        if 'partition_by' in self.options:
            writer = writer.partitionBy(self.options['partition_by'])
        
        writer.save(abfs_path)
        
        self.logger.info("Successfully wrote data to blob storage")
        
        return {
            "status": "success",
            "destination": abfs_path,
            "format": file_format,
            "mode": mode
        }
    
    def _write_data_lake(self, df) -> Dict[str, Any]:
        """Write to Azure Data Lake Gen2"""
        return self._write_blob_storage(df)
    
    def _write_synapse(self, df) -> Dict[str, Any]:
        """Write to Azure Synapse Analytics"""
        self.logger.info("Writing to Synapse Analytics")
        
        jdbc_url = self.options.get('jdbc_url')
        table = self.options.get('table')
        mode = self.options.get('mode', 'overwrite')
        
        if not jdbc_url or not table:
            raise ValueError("jdbc_url and table are required for Synapse connection")
        
        (df.write
         .format("com.databricks.spark.sqldw")
         .option("url", jdbc_url)
         .option("tempDir", f"abfss://{self.container}@{self.config.storage_account}.dfs.core.windows.net/temp")
         .option("forwardSparkAzureStorageCredentials", "true")
         .option("dbTable", table)
         .mode(mode)
         .save())
        
        self.logger.info(f"Successfully wrote data to Synapse table: {table}")
        
        return {
            "status": "success",
            "destination": f"synapse://{table}",
            "mode": mode
        }


class AzureEventHubConnector:
    """
    Connector for Azure Event Hubs streaming data.
    
    Example:
        >>> connector = AzureEventHubConnector(
        ...     namespace="my-namespace",
        ...     event_hub="events",
        ...     consumer_group="$Default"
        ... )
    """
    
    def __init__(
        self,
        namespace: str,
        event_hub: str,
        consumer_group: str = "$Default",
        connection_string: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Event Hub connector.
        
        Args:
            namespace: Event Hub namespace
            event_hub: Event Hub name
            consumer_group: Consumer group name
            connection_string: Connection string (optional if using managed identity)
        """
        self.namespace = namespace
        self.event_hub = event_hub
        self.consumer_group = consumer_group
        self.connection_string = connection_string
        self.options = kwargs
        
        self.logger = logging.getLogger("azure.eventhub")
        self.logger.info(f"Event Hub connector initialized: {namespace}/{event_hub}")
    
    def read_stream(self, spark):
        """Read streaming data from Event Hub"""
        self.logger.info(f"Reading stream from Event Hub: {self.event_hub}")
        
        connection_string = (self.connection_string or 
                           f"Endpoint=sb://{self.namespace}.servicebus.windows.net/;...")
        
        eh_conf = {
            'eventhubs.connectionString': connection_string,
            'eventhubs.consumerGroup': self.consumer_group,
            'eventhubs.startingPosition': self.options.get('starting_position', 'latest')
        }
        
        df = (spark.readStream
              .format("eventhubs")
              .options(**eh_conf)
              .load())
        
        self.logger.info("Stream reading initiated")
        return df
    
    def write_stream(self, df, checkpoint_location: str):
        """Write streaming data to Event Hub"""
        self.logger.info(f"Writing stream to Event Hub: {self.event_hub}")
        
        connection_string = (self.connection_string or 
                           f"Endpoint=sb://{self.namespace}.servicebus.windows.net/;...")
        
        eh_conf = {
            'eventhubs.connectionString': connection_string
        }
        
        query = (df.writeStream
                 .format("eventhubs")
                 .options(**eh_conf)
                 .option("checkpointLocation", checkpoint_location)
                 .start())
        
        self.logger.info("Stream writing initiated")
        return query


class AzureSQLConnector:
    """
    Connector for Azure SQL Database.
    
    Example:
        >>> connector = AzureSQLConnector(
        ...     server="myserver.database.windows.net",
        ...     database="mydb",
        ...     table="sales"
        ... )
    """
    
    def __init__(
        self,
        server: str,
        database: str,
        table: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_managed_identity: bool = True,
        **kwargs
    ):
        """
        Initialize Azure SQL connector.
        
        Args:
            server: SQL Server hostname
            database: Database name
            table: Table name
            username: Username (if not using managed identity)
            password: Password (if not using managed identity)
            use_managed_identity: Use Azure AD managed identity
        """
        self.server = server
        self.database = database
        self.table = table
        self.username = username
        self.password = password
        self.use_managed_identity = use_managed_identity
        self.options = kwargs
        
        self.logger = logging.getLogger("azure.sql")
        self.logger.info(f"Azure SQL connector initialized: {database}.{table}")
    
    def read(self, spark):
        """Read from Azure SQL"""
        self.logger.info(f"Reading from Azure SQL: {self.database}.{self.table}")
        
        jdbc_url = f"jdbc:sqlserver://{self.server}:1433;database={self.database}"
        
        if self.use_managed_identity:
            jdbc_url += ";Authentication=ActiveDirectoryMSI"
        
        reader = (spark.read
                  .format("jdbc")
                  .option("url", jdbc_url)
                  .option("dbtable", self.table)
                  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"))
        
        if not self.use_managed_identity:
            reader = reader.option("user", self.username).option("password", self.password)
        
        df = reader.load()
        
        self.logger.info("Successfully read data from Azure SQL")
        return df
    
    def write(self, df) -> Dict[str, Any]:
        """Write to Azure SQL"""
        self.logger.info(f"Writing to Azure SQL: {self.database}.{self.table}")
        
        jdbc_url = f"jdbc:sqlserver://{self.server}:1433;database={self.database}"
        
        if self.use_managed_identity:
            jdbc_url += ";Authentication=ActiveDirectoryMSI"
        
        mode = self.options.get('mode', 'overwrite')
        
        writer = (df.write
                  .format("jdbc")
                  .option("url", jdbc_url)
                  .option("dbtable", self.table)
                  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                  .mode(mode))
        
        if not self.use_managed_identity:
            writer = writer.option("user", self.username).option("password", self.password)
        
        writer.save()
        
        self.logger.info("Successfully wrote data to Azure SQL")
        
        return {
            "status": "success",
            "destination": f"sql://{self.server}/{self.database}.{self.table}",
            "mode": mode
        }
