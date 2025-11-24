"""
PySpark transformation library for Multi-Cloud Pipeline Framework
Optimized transformations for both batch and streaming data
"""

from typing import Optional, Dict, Any, List
from abc import ABC, abstractmethod
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class BaseTransformer(ABC):
    """Base class for all transformers"""
    
    def __init__(self, name: Optional[str] = None):
        """Initialize transformer"""
        self.name = name or self.__class__.__name__
        self.logger = logging.getLogger(f"transformer.{self.name}")
    
    @abstractmethod
    def transform(self, df: DataFrame, spark=None) -> DataFrame:
        """Apply transformation to DataFrame"""
        pass


class SparkTransformer(BaseTransformer):
    """
    Generic PySpark transformer with common operations.
    
    Example:
        >>> transformer = SparkTransformer(
        ...     transformation_type="aggregate",
        ...     group_by=["product_id", "date"],
        ...     aggregations={"revenue": "sum", "quantity": "sum"}
        ... )
    """
    
    def __init__(
        self,
        transformation_type: str,
        group_by: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, str]] = None,
        filter_condition: Optional[str] = None,
        select_columns: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Initialize Spark transformer.
        
        Args:
            transformation_type: Type of transformation (aggregate, filter, select, join, etc.)
            group_by: Columns to group by (for aggregations)
            aggregations: Dictionary of column:operation pairs
            filter_condition: SQL filter condition
            select_columns: Columns to select
        """
        super().__init__()
        self.transformation_type = transformation_type
        self.group_by = group_by or []
        self.aggregations = aggregations or {}
        self.filter_condition = filter_condition
        self.select_columns = select_columns
        self.options = kwargs
    
    def transform(self, df: DataFrame, spark=None) -> DataFrame:
        """Apply transformation based on type"""
        self.logger.info(f"Applying {self.transformation_type} transformation")
        
        if self.transformation_type == "aggregate":
            return self._apply_aggregation(df)
        elif self.transformation_type == "filter":
            return self._apply_filter(df)
        elif self.transformation_type == "select":
            return self._apply_select(df)
        elif self.transformation_type == "deduplicate":
            return self._apply_deduplication(df)
        elif self.transformation_type == "window":
            return self._apply_window(df)
        else:
            raise ValueError(f"Unsupported transformation type: {self.transformation_type}")
    
    def _apply_aggregation(self, df: DataFrame) -> DataFrame:
        """Apply aggregation transformation"""
        if not self.group_by:
            raise ValueError("group_by columns required for aggregation")
        
        # Build aggregation expressions
        agg_exprs = []
        for col, operation in self.aggregations.items():
            if operation == "sum":
                agg_exprs.append(F.sum(col).alias(f"{col}_sum"))
            elif operation == "avg":
                agg_exprs.append(F.avg(col).alias(f"{col}_avg"))
            elif operation == "count":
                agg_exprs.append(F.count(col).alias(f"{col}_count"))
            elif operation == "min":
                agg_exprs.append(F.min(col).alias(f"{col}_min"))
            elif operation == "max":
                agg_exprs.append(F.max(col).alias(f"{col}_max"))
            elif operation == "count_distinct":
                agg_exprs.append(F.countDistinct(col).alias(f"{col}_distinct"))
        
        result = df.groupBy(*self.group_by).agg(*agg_exprs)
        
        self.logger.info(f"Aggregation applied on {len(self.group_by)} columns")
        return result
    
    def _apply_filter(self, df: DataFrame) -> DataFrame:
        """Apply filter transformation"""
        if not self.filter_condition:
            raise ValueError("filter_condition required for filter transformation")
        
        result = df.filter(self.filter_condition)
        
        self.logger.info(f"Filter applied: {self.filter_condition}")
        return result
    
    def _apply_select(self, df: DataFrame) -> DataFrame:
        """Apply column selection"""
        if not self.select_columns:
            raise ValueError("select_columns required for select transformation")
        
        result = df.select(*self.select_columns)
        
        self.logger.info(f"Selected {len(self.select_columns)} columns")
        return result
    
    def _apply_deduplication(self, df: DataFrame) -> DataFrame:
        """Remove duplicate rows"""
        subset = self.options.get('subset')
        
        if subset:
            result = df.dropDuplicates(subset)
            self.logger.info(f"Deduplication applied on columns: {subset}")
        else:
            result = df.distinct()
            self.logger.info("Full deduplication applied")
        
        return result
    
    def _apply_window(self, df: DataFrame) -> DataFrame:
        """Apply window function"""
        partition_by = self.options.get('partition_by', [])
        order_by = self.options.get('order_by', [])
        window_function = self.options.get('window_function', 'row_number')
        
        window_spec = Window.partitionBy(*partition_by).orderBy(*order_by)
        
        if window_function == 'row_number':
            result = df.withColumn('row_num', F.row_number().over(window_spec))
        elif window_function == 'rank':
            result = df.withColumn('rank', F.rank().over(window_spec))
        elif window_function == 'dense_rank':
            result = df.withColumn('dense_rank', F.dense_rank().over(window_spec))
        else:
            raise ValueError(f"Unsupported window function: {window_function}")
        
        self.logger.info(f"Window function {window_function} applied")
        return result


class DataCleaningTransformer(BaseTransformer):
    """
    Transformer for data cleaning operations.
    
    Example:
        >>> transformer = DataCleaningTransformer(
        ...     remove_nulls=["customer_id", "order_date"],
        ...     standardize_columns=True
        ... )
    """
    
    def __init__(
        self,
        remove_nulls: Optional[List[str]] = None,
        fill_nulls: Optional[Dict[str, Any]] = None,
        remove_duplicates: bool = False,
        standardize_columns: bool = True,
        trim_strings: bool = True,
        **kwargs
    ):
        """
        Initialize data cleaning transformer.
        
        Args:
            remove_nulls: Columns to check for nulls (rows with nulls removed)
            fill_nulls: Dictionary of column:value pairs for filling nulls
            remove_duplicates: Whether to remove duplicate rows
            standardize_columns: Standardize column names (lowercase, underscores)
            trim_strings: Trim whitespace from string columns
        """
        super().__init__()
        self.remove_nulls = remove_nulls or []
        self.fill_nulls = fill_nulls or {}
        self.remove_duplicates = remove_duplicates
        self.standardize_columns = standardize_columns
        self.trim_strings = trim_strings
        self.options = kwargs
    
    def transform(self, df: DataFrame, spark=None) -> DataFrame:
        """Apply data cleaning transformations"""
        self.logger.info("Applying data cleaning transformations")
        
        result = df
        
        # Standardize column names
        if self.standardize_columns:
            result = self._standardize_column_names(result)
        
        # Remove nulls
        if self.remove_nulls:
            result = result.dropna(subset=self.remove_nulls)
            self.logger.info(f"Removed nulls from {len(self.remove_nulls)} columns")
        
        # Fill nulls
        if self.fill_nulls:
            result = result.fillna(self.fill_nulls)
            self.logger.info(f"Filled nulls in {len(self.fill_nulls)} columns")
        
        # Remove duplicates
        if self.remove_duplicates:
            result = result.distinct()
            self.logger.info("Removed duplicate rows")
        
        # Trim strings
        if self.trim_strings:
            result = self._trim_string_columns(result)
        
        return result
    
    def _standardize_column_names(self, df: DataFrame) -> DataFrame:
        """Standardize column names to lowercase with underscores"""
        new_columns = {}
        for col in df.columns:
            new_col = col.lower().replace(' ', '_').replace('-', '_')
            if new_col != col:
                new_columns[col] = new_col
        
        if new_columns:
            for old_col, new_col in new_columns.items():
                df = df.withColumnRenamed(old_col, new_col)
            self.logger.info(f"Standardized {len(new_columns)} column names")
        
        return df
    
    def _trim_string_columns(self, df: DataFrame) -> DataFrame:
        """Trim whitespace from string columns"""
        from pyspark.sql.types import StringType
        
        string_columns = [field.name for field in df.schema.fields 
                         if isinstance(field.dataType, StringType)]
        
        for col in string_columns:
            df = df.withColumn(col, F.trim(F.col(col)))
        
        if string_columns:
            self.logger.info(f"Trimmed {len(string_columns)} string columns")
        
        return df


class FeatureEngineeringTransformer(BaseTransformer):
    """
    Transformer for ML feature engineering.
    
    Example:
        >>> transformer = FeatureEngineeringTransformer(
        ...     date_features=["order_date"],
        ...     encoding_columns=["category", "region"]
        ... )
    """
    
    def __init__(
        self,
        date_features: Optional[List[str]] = None,
        encoding_columns: Optional[List[str]] = None,
        scaling_columns: Optional[List[str]] = None,
        binning: Optional[Dict[str, List[float]]] = None,
        **kwargs
    ):
        """
        Initialize feature engineering transformer.
        
        Args:
            date_features: Date columns to extract features from
            encoding_columns: Categorical columns to encode
            scaling_columns: Numeric columns to scale
            binning: Dictionary of column:[bins] for binning numeric values
        """
        super().__init__()
        self.date_features = date_features or []
        self.encoding_columns = encoding_columns or []
        self.scaling_columns = scaling_columns or []
        self.binning = binning or {}
        self.options = kwargs
    
    def transform(self, df: DataFrame, spark=None) -> DataFrame:
        """Apply feature engineering transformations"""
        self.logger.info("Applying feature engineering transformations")
        
        result = df
        
        # Extract date features
        for col in self.date_features:
            result = self._extract_date_features(result, col)
        
        # Apply binning
        for col, bins in self.binning.items():
            result = self._apply_binning(result, col, bins)
        
        self.logger.info("Feature engineering completed")
        return result
    
    def _extract_date_features(self, df: DataFrame, date_col: str) -> DataFrame:
        """Extract features from date column"""
        df = df.withColumn(f"{date_col}_year", F.year(date_col))
        df = df.withColumn(f"{date_col}_month", F.month(date_col))
        df = df.withColumn(f"{date_col}_day", F.dayofmonth(date_col))
        df = df.withColumn(f"{date_col}_dayofweek", F.dayofweek(date_col))
        df = df.withColumn(f"{date_col}_quarter", F.quarter(date_col))
        
        self.logger.info(f"Extracted date features from {date_col}")
        return df
    
    def _apply_binning(self, df: DataFrame, col: str, bins: List[float]) -> DataFrame:
        """Apply binning to numeric column"""
        # Create bucketizer
        from pyspark.ml.feature import Bucketizer
        
        bucketizer = Bucketizer(
            splits=bins,
            inputCol=col,
            outputCol=f"{col}_binned"
        )
        
        df = bucketizer.transform(df)
        
        self.logger.info(f"Applied binning to {col}")
        return df


class JoinTransformer(BaseTransformer):
    """
    Transformer for joining DataFrames.
    
    Example:
        >>> transformer = JoinTransformer(
        ...     right_df=customers_df,
        ...     on="customer_id",
        ...     how="left"
        ... )
    """
    
    def __init__(
        self,
        right_df: DataFrame,
        on: Optional[Any] = None,
        how: str = "inner",
        **kwargs
    ):
        """
        Initialize join transformer.
        
        Args:
            right_df: DataFrame to join with
            on: Column(s) to join on
            how: Join type (inner, left, right, outer, cross)
        """
        super().__init__()
        self.right_df = right_df
        self.on = on
        self.how = how
        self.options = kwargs
    
    def transform(self, df: DataFrame, spark=None) -> DataFrame:
        """Apply join transformation"""
        self.logger.info(f"Applying {self.how} join")
        
        result = df.join(self.right_df, on=self.on, how=self.how)
        
        self.logger.info("Join completed successfully")
        return result


class SQLTransformer(BaseTransformer):
    """
    Transformer using raw SQL queries.
    
    Example:
        >>> transformer = SQLTransformer(
        ...     sql_query=\"\"\"
        ...         SELECT product_id, SUM(revenue) as total_revenue
        ...         FROM sales
        ...         GROUP BY product_id
        ...     \"\"\"
        ... )
    """
    
    def __init__(self, sql_query: str, temp_view_name: str = "temp_table"):
        """
        Initialize SQL transformer.
        
        Args:
            sql_query: SQL query to execute
            temp_view_name: Name for temporary view
        """
        super().__init__()
        self.sql_query = sql_query
        self.temp_view_name = temp_view_name
    
    def transform(self, df: DataFrame, spark) -> DataFrame:
        """Apply SQL transformation"""
        if not spark:
            raise ValueError("Spark session required for SQL transformation")
        
        self.logger.info("Applying SQL transformation")
        
        # Create temporary view
        df.createOrReplaceTempView(self.temp_view_name)
        
        # Execute query
        result = spark.sql(self.sql_query)
        
        self.logger.info("SQL transformation completed")
        return result
