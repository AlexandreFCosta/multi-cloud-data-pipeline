"""
Unit tests for Pipeline class
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from multicloud_pipeline import Pipeline, CloudProvider


class TestPipeline:
    """Test cases for Pipeline class"""
    
    def test_pipeline_initialization(self):
        """Test pipeline can be initialized correctly"""
        pipeline = Pipeline(
            name="test_pipeline",
            cloud_provider="azure"
        )
        
        assert pipeline.name == "test_pipeline"
        assert pipeline.cloud_provider == CloudProvider.AZURE
        assert len(pipeline.sources) == 0
        assert len(pipeline.transformers) == 0
        assert len(pipeline.sinks) == 0
    
    def test_pipeline_initialization_with_gcp(self):
        """Test pipeline initialization with GCP"""
        pipeline = Pipeline(
            name="gcp_pipeline",
            cloud_provider="gcp"
        )
        
        assert pipeline.cloud_provider == CloudProvider.GCP
    
    def test_invalid_cloud_provider(self):
        """Test pipeline raises error for invalid cloud provider"""
        with pytest.raises(ValueError):
            Pipeline(
                name="invalid_pipeline",
                cloud_provider="aws"  # Not supported
            )
    
    def test_add_source(self):
        """Test adding source to pipeline"""
        pipeline = Pipeline(name="test", cloud_provider="azure")
        
        mock_source = Mock()
        pipeline.add_source(mock_source)
        
        assert len(pipeline.sources) == 1
        assert pipeline.sources[0] == mock_source
    
    def test_add_transformer(self):
        """Test adding transformer to pipeline"""
        pipeline = Pipeline(name="test", cloud_provider="azure")
        
        mock_transformer = Mock()
        pipeline.add_transformer(mock_transformer)
        
        assert len(pipeline.transformers) == 1
        assert pipeline.transformers[0] == mock_transformer
    
    def test_add_sink(self):
        """Test adding sink to pipeline"""
        pipeline = Pipeline(name="test", cloud_provider="azure")
        
        mock_sink = Mock()
        pipeline.add_sink(mock_sink)
        
        assert len(pipeline.sinks) == 1
        assert pipeline.sinks[0] == mock_sink
    
    def test_add_quality_check(self):
        """Test adding quality check to pipeline"""
        pipeline = Pipeline(name="test", cloud_provider="azure")
        
        mock_check = Mock()
        pipeline.add_quality_check(mock_check)
        
        assert len(pipeline.quality_checks) == 1
        assert pipeline.quality_checks[0] == mock_check
    
    def test_method_chaining(self):
        """Test that methods return self for chaining"""
        pipeline = Pipeline(name="test", cloud_provider="azure")
        
        result = (pipeline
                 .add_source(Mock())
                 .add_transformer(Mock())
                 .add_sink(Mock()))
        
        assert result is pipeline
        assert len(pipeline.sources) == 1
        assert len(pipeline.transformers) == 1
        assert len(pipeline.sinks) == 1
    
    def test_validation_fails_without_sources(self):
        """Test validation fails when no sources defined"""
        pipeline = Pipeline(name="test", cloud_provider="azure")
        
        assert not pipeline.validate()
    
    def test_validation_passes_with_sources(self):
        """Test validation passes with sources"""
        pipeline = Pipeline(name="test", cloud_provider="azure")
        pipeline.add_source(Mock())
        
        assert pipeline.validate()
    
    def test_dry_run(self):
        """Test dry run returns validation information"""
        pipeline = Pipeline(name="test", cloud_provider="azure")
        pipeline.add_source(Mock())
        pipeline.add_transformer(Mock())
        
        result = pipeline.dry_run()
        
        assert result['pipeline_name'] == "test"
        assert result['cloud_provider'] == "azure"
        assert result['sources'] == 1
        assert result['transformers'] == 1
        assert result['valid'] is True
    
    def test_get_lineage(self):
        """Test getting pipeline lineage"""
        pipeline = Pipeline(name="test", cloud_provider="azure", enable_lineage=True)
        
        mock_source = Mock()
        mock_transformer = Mock()
        mock_sink = Mock()
        
        pipeline.add_source(mock_source)
        pipeline.add_transformer(mock_transformer)
        pipeline.add_sink(mock_sink)
        
        lineage = pipeline.get_lineage()
        
        assert lineage['pipeline'] == "test"
        assert len(lineage['sources']) == 1
        assert len(lineage['transformations']) == 1
        assert len(lineage['sinks']) == 1
    
    def test_get_lineage_disabled(self):
        """Test lineage when disabled"""
        pipeline = Pipeline(name="test", cloud_provider="azure", enable_lineage=False)
        
        lineage = pipeline.get_lineage()
        
        assert lineage.get('lineage_disabled') is True
    
    @patch('multicloud_pipeline.SparkSession')
    def test_run_pipeline_success(self, mock_spark_session):
        """Test successful pipeline execution"""
        # Setup mocks
        mock_spark = Mock()
        mock_spark_session.builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_spark
        
        mock_source = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_source.read.return_value = mock_df
        
        mock_transformer = Mock()
        mock_transformer.transform.return_value = mock_df
        
        mock_sink = Mock()
        mock_sink.write.return_value = {"status": "success"}
        
        # Create and execute pipeline
        pipeline = Pipeline(name="test", cloud_provider="azure")
        pipeline.add_source(mock_source)
        pipeline.add_transformer(mock_transformer)
        pipeline.add_sink(mock_sink)
        
        result = pipeline.run(spark_session=mock_spark)
        
        # Assertions
        assert result['status'] == 'success'
        assert result['pipeline_name'] == 'test'
        assert result['records_processed'] == 100
        assert 'duration_seconds' in result
        
        mock_source.read.assert_called_once()
        mock_transformer.transform.assert_called_once()
        mock_sink.write.assert_called_once()
    
    def test_run_pipeline_validation_failure(self):
        """Test pipeline run fails validation without sources"""
        pipeline = Pipeline(name="test", cloud_provider="azure")
        
        with pytest.raises(ValueError, match="Pipeline validation failed"):
            pipeline.run()


class TestStreamingPipeline:
    """Test cases for StreamingPipeline class"""
    
    def test_streaming_pipeline_initialization(self):
        """Test streaming pipeline initialization"""
        from multicloud_pipeline import StreamingPipeline
        
        pipeline = StreamingPipeline(
            name="streaming_test",
            cloud_provider="gcp",
            checkpoint_location="gs://bucket/checkpoint"
        )
        
        assert pipeline.name == "streaming_test"
        assert pipeline.checkpoint_location == "gs://bucket/checkpoint"
        assert pipeline.trigger == "processingTime='1 minute'"
    
    def test_streaming_pipeline_start(self):
        """Test starting streaming pipeline"""
        from multicloud_pipeline import StreamingPipeline
        
        pipeline = StreamingPipeline(
            name="streaming_test",
            cloud_provider="gcp"
        )
        pipeline.add_source(Mock())
        
        result = pipeline.start()
        
        assert result['status'] == 'running'
        assert result['pipeline'] == 'streaming_test'


class TestCrossCloudPipeline:
    """Test cases for CrossCloudPipeline class"""
    
    def test_cross_cloud_pipeline_initialization(self):
        """Test cross-cloud pipeline initialization"""
        from multicloud_pipeline import CrossCloudPipeline
        
        pipeline = CrossCloudPipeline(
            source_cloud="azure",
            target_cloud="gcp"
        )
        
        assert pipeline.source_cloud == CloudProvider.AZURE
        assert pipeline.target_cloud == CloudProvider.GCP
    
    def test_cross_cloud_migration(self):
        """Test cross-cloud migration"""
        from multicloud_pipeline import CrossCloudPipeline
        
        pipeline = CrossCloudPipeline(
            source_cloud="azure",
            target_cloud="gcp"
        )
        
        source_config = {
            "type": "azure_data_lake",
            "path": "abfss://data@storage.dfs.core.windows.net/customers/"
        }
        
        target_config = {
            "type": "bigquery",
            "dataset": "customer_data",
            "table": "customers"
        }
        
        result = pipeline.migrate(
            source=source_config,
            target=target_config,
            validation=True
        )
        
        assert result['status'] == 'success'
        assert result['source_cloud'] == 'azure'
        assert result['target_cloud'] == 'gcp'
        assert result['validation_performed'] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
