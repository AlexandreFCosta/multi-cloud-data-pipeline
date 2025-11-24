"""
Multi-Cloud Data Pipeline Framework

A production-ready framework for building scalable ETL/ELT pipelines
across Azure and GCP using PySpark.
"""

from enum import Enum
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CloudProvider(Enum):
    """Supported cloud providers"""
    AZURE = "azure"
    GCP = "gcp"
    AWS = "aws"


class PipelineStage:
    """Represents a single stage in the data pipeline"""
    
    def __init__(self, name: str, function: Callable, stage_type: str):
        self.name = name
        self.function = function
        self.stage_type = stage_type  # 'source', 'transformer', 'sink', 'quality'
        self.execution_time = None
        self.records_processed = 0
        self.status = "pending"
    
    def execute(self, data: Any) -> Any:
        """Execute the stage function"""
        start_time = datetime.now()
        try:
            result = self.function(data)
            self.status = "success"
            self.execution_time = (datetime.now() - start_time).total_seconds()
            return result
        except Exception as e:
            self.status = "failed"
            self.execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Stage {self.name} failed: {str(e)}")
            raise


class Pipeline:
    """
    Main Pipeline class for building and executing data pipelines.
    
    Example:
        >>> pipeline = Pipeline(name="sales_etl", cloud_provider=CloudProvider.AZURE)
        >>> pipeline.add_source(azure_blob_source)
        >>> pipeline.add_transformer(clean_data)
        >>> pipeline.add_sink(synapse_sink)
        >>> pipeline.run()
    """
    
    def __init__(
        self,
        name: str,
        cloud_provider: CloudProvider,
        spark_session: Optional[Any] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.cloud_provider = cloud_provider
        self.spark = spark_session
        self.config = config or {}
        self.stages: List[PipelineStage] = []
        self.lineage: List[Dict[str, Any]] = []
        self.metadata = {
            "created_at": datetime.now(),
            "cloud_provider": cloud_provider.value,
            "status": "initialized"
        }
        
        logger.info(f"Pipeline '{name}' initialized for {cloud_provider.value}")
    
    def add_source(
        self,
        connector: Any,
        name: Optional[str] = None
    ) -> 'Pipeline':
        """
        Add a data source to the pipeline
        
        Args:
            connector: Source connector (AzureBlobConnector, GCPConnector, etc.)
            name: Optional name for this source stage
            
        Returns:
            Self for method chaining
        """
        stage_name = name or f"source_{len([s for s in self.stages if s.stage_type == 'source']) + 1}"
        
        def source_function(data):
            return connector.read()
        
        stage = PipelineStage(stage_name, source_function, "source")
        self.stages.append(stage)
        
        self.lineage.append({
            "stage": stage_name,
            "type": "source",
            "connector": connector.__class__.__name__,
            "timestamp": datetime.now()
        })
        
        logger.info(f"Added source stage: {stage_name}")
        return self
    
    def add_transformer(
        self,
        transformer: Callable,
        name: Optional[str] = None
    ) -> 'Pipeline':
        """
        Add a transformation stage to the pipeline
        
        Args:
            transformer: Transformation function that takes and returns a DataFrame
            name: Optional name for this transformation stage
            
        Returns:
            Self for method chaining
        """
        stage_name = name or f"transform_{len([s for s in self.stages if s.stage_type == 'transformer']) + 1}"
        stage = PipelineStage(stage_name, transformer, "transformer")
        self.stages.append(stage)
        
        self.lineage.append({
            "stage": stage_name,
            "type": "transformer",
            "function": transformer.__name__ if hasattr(transformer, '__name__') else str(transformer),
            "timestamp": datetime.now()
        })
        
        logger.info(f"Added transformer stage: {stage_name}")
        return self
    
    def add_sink(
        self,
        connector: Any,
        name: Optional[str] = None
    ) -> 'Pipeline':
        """
        Add a data sink to the pipeline
        
        Args:
            connector: Sink connector (SynapseConnector, BigQueryConnector, etc.)
            name: Optional name for this sink stage
            
        Returns:
            Self for method chaining
        """
        stage_name = name or f"sink_{len([s for s in self.stages if s.stage_type == 'sink']) + 1}"
        
        def sink_function(data):
            connector.write(data)
            return data
        
        stage = PipelineStage(stage_name, sink_function, "sink")
        self.stages.append(stage)
        
        self.lineage.append({
            "stage": stage_name,
            "type": "sink",
            "connector": connector.__class__.__name__,
            "timestamp": datetime.now()
        })
        
        logger.info(f"Added sink stage: {stage_name}")
        return self
    
    def add_quality_check(
        self,
        check_function: Callable,
        name: Optional[str] = None
    ) -> 'Pipeline':
        """
        Add a data quality check to the pipeline
        
        Args:
            check_function: Quality check function that validates data
            name: Optional name for this quality check
            
        Returns:
            Self for method chaining
        """
        stage_name = name or f"quality_check_{len([s for s in self.stages if s.stage_type == 'quality']) + 1}"
        
        def quality_wrapper(data):
            check_result = check_function(data)
            if not check_result:
                raise ValueError(f"Quality check failed: {stage_name}")
            return data
        
        stage = PipelineStage(stage_name, quality_wrapper, "quality")
        self.stages.append(stage)
        
        self.lineage.append({
            "stage": stage_name,
            "type": "quality_check",
            "timestamp": datetime.now()
        })
        
        logger.info(f"Added quality check: {stage_name}")
        return self
    
    def validate(self) -> bool:
        """
        Validate pipeline configuration
        
        Returns:
            True if pipeline is valid, raises ValueError otherwise
        """
        if not self.stages:
            raise ValueError("Pipeline has no stages defined")
        
        # Check if pipeline has at least one source
        sources = [s for s in self.stages if s.stage_type == "source"]
        if not sources:
            raise ValueError("Pipeline must have at least one source")
        
        # Check if pipeline has at least one sink
        sinks = [s for s in self.stages if s.stage_type == "sink"]
        if not sinks:
            logger.warning("Pipeline has no sink stages - data will not be persisted")
        
        logger.info("Pipeline validation passed")
        return True
    
    def run(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Execute the pipeline
        
        Args:
            dry_run: If True, validates but doesn't execute the pipeline
            
        Returns:
            Dictionary with execution results and metadata
        """
        self.validate()
        
        if dry_run:
            logger.info("Dry run - pipeline validation successful")
            return {
                "status": "validated",
                "stages": len(self.stages),
                "lineage": self.lineage
            }
        
        logger.info(f"Starting pipeline execution: {self.name}")
        start_time = datetime.now()
        data = None
        
        try:
            for stage in self.stages:
                logger.info(f"Executing stage: {stage.name} ({stage.stage_type})")
                data = stage.execute(data)
            
            self.metadata["status"] = "completed"
            self.metadata["completed_at"] = datetime.now()
            self.metadata["execution_time"] = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Pipeline '{self.name}' completed successfully in {self.metadata['execution_time']:.2f}s")
            
            return {
                "status": "success",
                "pipeline": self.name,
                "execution_time": self.metadata["execution_time"],
                "stages_executed": len(self.stages),
                "metadata": self.metadata
            }
            
        except Exception as e:
            self.metadata["status"] = "failed"
            self.metadata["error"] = str(e)
            self.metadata["failed_at"] = datetime.now()
            
            logger.error(f"Pipeline '{self.name}' failed: {str(e)}")
            
            return {
                "status": "failed",
                "pipeline": self.name,
                "error": str(e),
                "metadata": self.metadata
            }
    
    def get_lineage(self) -> List[Dict[str, Any]]:
        """
        Get pipeline lineage information
        
        Returns:
            List of lineage records for each stage
        """
        return self.lineage
    
    def dry_run(self) -> Dict[str, Any]:
        """
        Perform a dry run (validation only)
        
        Returns:
            Validation results
        """
        return self.run(dry_run=True)


class StreamingPipeline(Pipeline):
    """
    Streaming pipeline for real-time data processing
    
    Example:
        >>> pipeline = StreamingPipeline(
        ...     name="realtime_events",
        ...     cloud_provider=CloudProvider.GCP,
        ...     checkpoint_location="/tmp/checkpoints"
        ... )
        >>> pipeline.add_source(pubsub_source)
        >>> pipeline.add_transformer(parse_json)
        >>> pipeline.add_sink(bigquery_sink)
        >>> pipeline.start()
    """
    
    def __init__(
        self,
        name: str,
        cloud_provider: CloudProvider,
        spark_session: Optional[Any] = None,
        checkpoint_location: Optional[str] = None,
        trigger: str = "processingTime='10 seconds'",
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(name, cloud_provider, spark_session, config)
        self.checkpoint_location = checkpoint_location or f"/tmp/checkpoints/{name}"
        self.trigger = trigger
        self.streaming_query = None
        self.metadata["pipeline_type"] = "streaming"
    
    def start(self) -> Any:
        """
        Start the streaming pipeline
        
        Returns:
            Streaming query object
        """
        logger.info(f"Starting streaming pipeline: {self.name}")
        self.validate()
        
        # In a real implementation, this would start the Spark Structured Streaming query
        # For now, we'll just log and return metadata
        self.metadata["status"] = "running"
        self.metadata["started_at"] = datetime.now()
        
        logger.info(f"Streaming pipeline '{self.name}' started with trigger: {self.trigger}")
        
        return {
            "status": "running",
            "pipeline": self.name,
            "checkpoint_location": self.checkpoint_location,
            "trigger": self.trigger
        }
    
    def stop(self):
        """Stop the streaming pipeline"""
        if self.streaming_query:
            self.streaming_query.stop()
            self.metadata["status"] = "stopped"
            self.metadata["stopped_at"] = datetime.now()
            logger.info(f"Streaming pipeline '{self.name}' stopped")


class CrossCloudPipeline(Pipeline):
    """
    Pipeline for cross-cloud data migration and synchronization
    
    Example:
        >>> pipeline = CrossCloudPipeline(
        ...     name="azure_to_gcp_sync",
        ...     source_cloud=CloudProvider.AZURE,
        ...     target_cloud=CloudProvider.GCP
        ... )
        >>> pipeline.add_source(azure_source)
        >>> pipeline.add_transformer(transform_schema)
        >>> pipeline.add_sink(gcp_sink)
        >>> pipeline.run()
    """
    
    def __init__(
        self,
        name: str,
        source_cloud: CloudProvider,
        target_cloud: CloudProvider,
        spark_session: Optional[Any] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(name, source_cloud, spark_session, config)
        self.source_cloud = source_cloud
        self.target_cloud = target_cloud
        self.metadata["pipeline_type"] = "cross_cloud"
        self.metadata["source_cloud"] = source_cloud.value
        self.metadata["target_cloud"] = target_cloud.value
        
        logger.info(f"Cross-cloud pipeline initialized: {source_cloud.value} → {target_cloud.value}")
    
    def validate(self) -> bool:
        """Validate cross-cloud pipeline configuration"""
        super().validate()
        
        if self.source_cloud == self.target_cloud:
            logger.warning("Source and target clouds are the same")
        
        return True


# Package version
__version__ = "1.0.0"

# Export main classes
__all__ = [
    "Pipeline",
    "StreamingPipeline",
    "CrossCloudPipeline",
    "CloudProvider",
    "PipelineStage"
]
