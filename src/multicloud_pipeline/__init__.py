"""
Multi-Cloud Data Pipeline Framework
Core Pipeline class for orchestrating data workflows across Azure and GCP
"""

from typing import Optional, Dict, List, Any
from enum import Enum
import logging
from datetime import datetime


class CloudProvider(Enum):
    """Supported cloud providers"""
    AZURE = "azure"
    GCP = "gcp"


class Pipeline:
    """
    Main pipeline orchestrator supporting both Azure and GCP environments.
    
    Example:
        >>> pipeline = Pipeline(name="sales_pipeline", cloud_provider="azure")
        >>> pipeline.add_source(source_connector)
        >>> pipeline.add_transformer(transformer)
        >>> pipeline.run()
    """
    
    def __init__(
        self,
        name: str,
        cloud_provider: str,
        config: Optional[Dict[str, Any]] = None,
        enable_monitoring: bool = True,
        enable_lineage: bool = True
    ):
        """
        Initialize a new pipeline.
        
        Args:
            name: Pipeline name
            cloud_provider: Cloud provider ('azure' or 'gcp')
            config: Optional configuration dictionary
            enable_monitoring: Enable monitoring and metrics
            enable_lineage: Enable data lineage tracking
        """
        self.name = name
        self.cloud_provider = CloudProvider(cloud_provider.lower())
        self.config = config or {}
        self.enable_monitoring = enable_monitoring
        self.enable_lineage = enable_lineage
        
        self.sources = []
        self.transformers = []
        self.sinks = []
        self.quality_checks = []
        
        self.logger = self._setup_logger()
        self.metadata = {
            "created_at": datetime.utcnow().isoformat(),
            "pipeline_name": name,
            "cloud_provider": cloud_provider
        }
        
        self.logger.info(f"Pipeline '{name}' initialized for {cloud_provider}")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger(f"pipeline.{self.name}")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def add_source(self, source: Any) -> 'Pipeline':
        """Add a data source connector"""
        self.sources.append(source)
        self.logger.info(f"Added source: {source.__class__.__name__}")
        return self
    
    def add_transformer(self, transformer: Any) -> 'Pipeline':
        """Add a transformation step"""
        self.transformers.append(transformer)
        self.logger.info(f"Added transformer: {transformer.__class__.__name__}")
        return self
    
    def add_sink(self, sink: Any) -> 'Pipeline':
        """Add a data sink/destination"""
        self.sinks.append(sink)
        self.logger.info(f"Added sink: {sink.__class__.__name__}")
        return self
    
    def add_quality_check(self, check: Any) -> 'Pipeline':
        """Add a data quality check"""
        self.quality_checks.append(check)
        self.logger.info(f"Added quality check: {check.__class__.__name__}")
        return self
    
    def validate(self) -> bool:
        """Validate pipeline configuration before execution"""
        if not self.sources:
            self.logger.error("Pipeline validation failed: No sources defined")
            return False
        
        if not self.sinks:
            self.logger.warning("No sinks defined - pipeline will process but not persist data")
        
        self.logger.info("Pipeline validation passed")
        return True
    
    def run(self, spark_session: Optional[Any] = None) -> Dict[str, Any]:
        """
        Execute the pipeline.
        
        Args:
            spark_session: Optional Spark session (will create if not provided)
            
        Returns:
            Dictionary with execution results and metrics
        """
        start_time = datetime.utcnow()
        self.logger.info(f"Starting pipeline execution: {self.name}")
        
        if not self.validate():
            raise ValueError("Pipeline validation failed")
        
        try:
            # Initialize Spark if not provided
            if spark_session is None:
                spark_session = self._get_spark_session()
            
            # Execute pipeline stages
            data = self._execute_sources(spark_session)
            data = self._execute_transformers(data, spark_session)
            self._execute_quality_checks(data)
            results = self._execute_sinks(data)
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            execution_results = {
                "status": "success",
                "pipeline_name": self.name,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "records_processed": data.count() if hasattr(data, 'count') else 0,
                "results": results
            }
            
            self.logger.info(
                f"Pipeline execution completed successfully in {duration:.2f}s"
            )
            
            return execution_results
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
            raise
    
    def _get_spark_session(self):
        """Get or create Spark session based on cloud provider"""
        try:
            from pyspark.sql import SparkSession
            
            builder = SparkSession.builder.appName(self.name)
            
            if self.cloud_provider == CloudProvider.AZURE:
                # Azure Databricks optimizations
                builder = builder.config("spark.databricks.delta.optimizeWrite.enabled", "true")
                builder = builder.config("spark.databricks.delta.autoCompact.enabled", "true")
            elif self.cloud_provider == CloudProvider.GCP:
                # GCP Dataproc optimizations
                builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
            
            return builder.getOrCreate()
            
        except ImportError:
            self.logger.warning("PySpark not available - using mock session")
            return None
    
    def _execute_sources(self, spark):
        """Execute all source connectors"""
        self.logger.info(f"Executing {len(self.sources)} source connector(s)")
        
        if not self.sources:
            return None
        
        # For simplicity, use first source. In production, support multiple sources
        data = self.sources[0].read(spark)
        self.logger.info(f"Source data loaded successfully")
        
        return data
    
    def _execute_transformers(self, data, spark):
        """Execute all transformations"""
        if not data:
            return data
        
        self.logger.info(f"Executing {len(self.transformers)} transformer(s)")
        
        for transformer in self.transformers:
            data = transformer.transform(data, spark)
            self.logger.info(f"Transformation {transformer.__class__.__name__} applied")
        
        return data
    
    def _execute_quality_checks(self, data):
        """Execute data quality checks"""
        if not self.quality_checks or not data:
            return
        
        self.logger.info(f"Executing {len(self.quality_checks)} quality check(s)")
        
        for check in self.quality_checks:
            result = check.validate(data)
            if not result.success:
                self.logger.error(f"Quality check failed: {result.message}")
                if check.blocking:
                    raise ValueError(f"Blocking quality check failed: {result.message}")
    
    def _execute_sinks(self, data) -> Dict[str, Any]:
        """Execute all sink connectors"""
        if not self.sinks or not data:
            return {}
        
        self.logger.info(f"Executing {len(self.sinks)} sink connector(s)")
        
        results = {}
        for sink in self.sinks:
            result = sink.write(data)
            results[sink.__class__.__name__] = result
            self.logger.info(f"Data written to {sink.__class__.__name__}")
        
        return results
    
    def dry_run(self) -> Dict[str, Any]:
        """
        Perform a dry run to validate pipeline without executing.
        
        Returns:
            Dictionary with validation results
        """
        self.logger.info("Performing dry run")
        
        validation = {
            "pipeline_name": self.name,
            "cloud_provider": self.cloud_provider.value,
            "sources": len(self.sources),
            "transformers": len(self.transformers),
            "sinks": len(self.sinks),
            "quality_checks": len(self.quality_checks),
            "valid": self.validate()
        }
        
        return validation
    
    def get_lineage(self) -> Dict[str, Any]:
        """Get data lineage information"""
        if not self.enable_lineage:
            return {"lineage_disabled": True}
        
        lineage = {
            "pipeline": self.name,
            "sources": [s.__class__.__name__ for s in self.sources],
            "transformations": [t.__class__.__name__ for t in self.transformers],
            "sinks": [s.__class__.__name__ for s in self.sinks],
            "metadata": self.metadata
        }
        
        return lineage


class StreamingPipeline(Pipeline):
    """
    Streaming pipeline for real-time data processing.
    
    Extends the base Pipeline class with streaming-specific functionality.
    """
    
    def __init__(
        self,
        name: str,
        cloud_provider: str,
        checkpoint_location: Optional[str] = None,
        trigger: str = "processingTime='1 minute'",
        **kwargs
    ):
        """
        Initialize streaming pipeline.
        
        Args:
            name: Pipeline name
            cloud_provider: Cloud provider
            checkpoint_location: Checkpoint directory for fault tolerance
            trigger: Streaming trigger configuration
        """
        super().__init__(name, cloud_provider, **kwargs)
        self.checkpoint_location = checkpoint_location
        self.trigger = trigger
        self.streaming_query = None
        
        self.logger.info(f"Streaming pipeline '{name}' initialized with trigger: {trigger}")
    
    def start(self):
        """Start the streaming pipeline"""
        self.logger.info(f"Starting streaming pipeline: {self.name}")
        
        if not self.validate():
            raise ValueError("Pipeline validation failed")
        
        # In a real implementation, this would start the streaming query
        self.logger.info("Streaming pipeline started successfully")
        
        return {
            "status": "running",
            "pipeline": self.name,
            "trigger": self.trigger
        }
    
    def stop(self):
        """Stop the streaming pipeline"""
        self.logger.info(f"Stopping streaming pipeline: {self.name}")
        
        if self.streaming_query:
            self.streaming_query.stop()
        
        self.logger.info("Streaming pipeline stopped")


class CrossCloudPipeline:
    """
    Pipeline for cross-cloud data migration and synchronization.
    
    Facilitates data transfer between Azure and GCP.
    """
    
    def __init__(self, source_cloud: str, target_cloud: str):
        """
        Initialize cross-cloud pipeline.
        
        Args:
            source_cloud: Source cloud provider
            target_cloud: Target cloud provider
        """
        self.source_cloud = CloudProvider(source_cloud.lower())
        self.target_cloud = CloudProvider(target_cloud.lower())
        self.logger = logging.getLogger("pipeline.cross_cloud")
        
        self.logger.info(
            f"Cross-cloud pipeline initialized: {source_cloud} -> {target_cloud}"
        )
    
    def migrate(
        self,
        source: Dict[str, Any],
        target: Dict[str, Any],
        transformation: Optional[str] = None,
        validation: bool = True
    ) -> Dict[str, Any]:
        """
        Execute cross-cloud migration.
        
        Args:
            source: Source configuration
            target: Target configuration
            transformation: Optional transformation to apply during migration
            validation: Whether to validate data after migration
            
        Returns:
            Migration results
        """
        self.logger.info("Starting cross-cloud migration")
        
        start_time = datetime.utcnow()
        
        # In production, implement actual data transfer logic
        results = {
            "status": "success",
            "source_cloud": self.source_cloud.value,
            "target_cloud": self.target_cloud.value,
            "start_time": start_time.isoformat(),
            "transformation_applied": transformation is not None,
            "validation_performed": validation
        }
        
        self.logger.info("Cross-cloud migration completed")
        
        return results
