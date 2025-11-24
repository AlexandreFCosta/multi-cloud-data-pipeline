"""
PySpark Transformers

Collection of transformers for data cleaning, feature engineering,
and common transformations optimized for PySpark.
"""

from typing import List, Dict, Any, Optional, Callable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


class SparkTransformer:
    """
    Base transformer class with common PySpark operations
    
    Example:
        >>> transformer = SparkTransformer()
        >>> df_clean = transformer.select_columns(df, ["id", "name", "amount"])
        >>> df_agg = transformer.aggregate(df, ["category"], {"amount": "sum"})
    """
    
    @staticmethod
    def select_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """Select specific columns from DataFrame"""
        logger.info(f"Selecting columns: {columns}")
        return df.select(*columns)
    
    @staticmethod
    def filter_data(df: DataFrame, condition: str) -> DataFrame:
        """
        Filter DataFrame based on SQL condition
        
        Args:
            df: Input DataFrame
            condition: SQL where condition (e.g., "age > 18 AND status = 'active'")
        """
        logger.info(f"Filtering data with condition: {condition}")
        return df.filter(condition)
    
    @staticmethod
    def aggregate(
        df: DataFrame,
        group_by: List[str],
        aggregations: Dict[str, str]
    ) -> DataFrame:
        """
        Perform group by aggregation
        
        Args:
            df: Input DataFrame
            group_by: List of columns to group by
            aggregations: Dictionary of {column: agg_function}
                         e.g., {"sales": "sum", "quantity": "avg"}
        """
        logger.info(f"Aggregating by {group_by} with {aggregations}")
        
        agg_exprs = []
        for col, func in aggregations.items():
            if func == "sum":
                agg_exprs.append(F.sum(col).alias(f"{col}_sum"))
            elif func == "avg" or func == "mean":
                agg_exprs.append(F.avg(col).alias(f"{col}_avg"))
            elif func == "count":
                agg_exprs.append(F.count(col).alias(f"{col}_count"))
            elif func == "min":
                agg_exprs.append(F.min(col).alias(f"{col}_min"))
            elif func == "max":
                agg_exprs.append(F.max(col).alias(f"{col}_max"))
        
        return df.groupBy(*group_by).agg(*agg_exprs)
    
    @staticmethod
    def deduplicate(
        df: DataFrame,
        columns: Optional[List[str]] = None,
        keep: str = "first"
    ) -> DataFrame:
        """
        Remove duplicate rows
        
        Args:
            df: Input DataFrame
            columns: Columns to consider for duplicates (None = all columns)
            keep: Which duplicate to keep ('first' or 'last')
        """
        logger.info(f"Deduplicating data based on {columns or 'all columns'}")
        
        if columns is None:
            return df.dropDuplicates()
        
        if keep == "first":
            window = Window.partitionBy(*columns).orderBy(F.monotonically_increasing_id())
            return df.withColumn("row_num", F.row_number().over(window)) \
                     .filter(F.col("row_num") == 1) \
                     .drop("row_num")
        else:
            window = Window.partitionBy(*columns).orderBy(F.monotonically_increasing_id().desc())
            return df.withColumn("row_num", F.row_number().over(window)) \
                     .filter(F.col("row_num") == 1) \
                     .drop("row_num")
    
    @staticmethod
    def add_window_functions(
        df: DataFrame,
        partition_by: List[str],
        order_by: List[str],
        functions: Dict[str, str]
    ) -> DataFrame:
        """
        Add window function columns
        
        Args:
            df: Input DataFrame
            partition_by: Columns to partition by
            order_by: Columns to order by
            functions: Dictionary of {new_col_name: window_function}
                      e.g., {"row_num": "row_number", "rank": "rank"}
        """
        logger.info(f"Adding window functions partitioned by {partition_by}")
        
        window = Window.partitionBy(*partition_by).orderBy(*order_by)
        
        result_df = df
        for col_name, func in functions.items():
            if func == "row_number":
                result_df = result_df.withColumn(col_name, F.row_number().over(window))
            elif func == "rank":
                result_df = result_df.withColumn(col_name, F.rank().over(window))
            elif func == "dense_rank":
                result_df = result_df.withColumn(col_name, F.dense_rank().over(window))
            elif func == "lag":
                result_df = result_df.withColumn(col_name, F.lag(order_by[0], 1).over(window))
            elif func == "lead":
                result_df = result_df.withColumn(col_name, F.lead(order_by[0], 1).over(window))
        
        return result_df


class DataCleaningTransformer:
    """
    Transformer for data cleaning operations
    
    Example:
        >>> cleaner = DataCleaningTransformer()
        >>> df_clean = cleaner.remove_nulls(df, ["id", "email"])
        >>> df_clean = cleaner.standardize_column_names(df_clean)
    """
    
    @staticmethod
    def remove_nulls(df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
        """
        Remove rows with null values
        
        Args:
            df: Input DataFrame
            columns: Specific columns to check (None = all columns)
        """
        logger.info(f"Removing nulls from {columns or 'all columns'}")
        
        if columns:
            return df.dropna(subset=columns)
        return df.dropna()
    
    @staticmethod
    def fill_nulls(
        df: DataFrame,
        fill_values: Dict[str, Any],
        strategy: str = "value"
    ) -> DataFrame:
        """
        Fill null values
        
        Args:
            df: Input DataFrame
            fill_values: Dictionary of {column: fill_value} or default value
            strategy: 'value', 'mean', 'median', 'mode'
        """
        logger.info(f"Filling nulls with strategy: {strategy}")
        
        if strategy == "value":
            return df.fillna(fill_values)
        elif strategy == "mean":
            # Calculate means for numeric columns
            means = {}
            for col in fill_values.keys():
                mean_val = df.select(F.mean(col)).first()[0]
                means[col] = mean_val
            return df.fillna(means)
        elif strategy == "median":
            # Calculate medians
            medians = {}
            for col in fill_values.keys():
                median_val = df.approxQuantile(col, [0.5], 0.01)[0]
                medians[col] = median_val
            return df.fillna(medians)
        else:
            return df.fillna(fill_values)
    
    @staticmethod
    def standardize_column_names(df: DataFrame) -> DataFrame:
        """Standardize column names (lowercase, remove spaces)"""
        logger.info("Standardizing column names")
        
        new_cols = [col.lower().replace(" ", "_").replace("-", "_") for col in df.columns]
        return df.toDF(*new_cols)
    
    @staticmethod
    def trim_strings(df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
        """
        Trim whitespace from string columns
        
        Args:
            df: Input DataFrame
            columns: Specific columns to trim (None = all string columns)
        """
        logger.info("Trimming string columns")
        
        if columns is None:
            # Get all string columns
            columns = [field.name for field in df.schema.fields 
                      if str(field.dataType) == "StringType"]
        
        result_df = df
        for col in columns:
            result_df = result_df.withColumn(col, F.trim(F.col(col)))
        
        return result_df
    
    @staticmethod
    def remove_duplicates(
        df: DataFrame,
        columns: Optional[List[str]] = None
    ) -> DataFrame:
        """Remove duplicate rows"""
        logger.info(f"Removing duplicates based on {columns or 'all columns'}")
        
        if columns:
            return df.dropDuplicates(columns)
        return df.dropDuplicates()
    
    @staticmethod
    def cast_columns(df: DataFrame, cast_dict: Dict[str, str]) -> DataFrame:
        """
        Cast columns to specific types
        
        Args:
            df: Input DataFrame
            cast_dict: Dictionary of {column: type}
                      e.g., {"age": "integer", "salary": "double"}
        """
        logger.info(f"Casting columns: {cast_dict}")
        
        result_df = df
        for col, dtype in cast_dict.items():
            result_df = result_df.withColumn(col, F.col(col).cast(dtype))
        
        return result_df


class FeatureEngineeringTransformer:
    """
    Transformer for feature engineering operations
    
    Example:
        >>> fe = FeatureEngineeringTransformer()
        >>> df = fe.add_date_features(df, "order_date")
        >>> df = fe.create_bins(df, "age", [0, 18, 35, 50, 100])
    """
    
    @staticmethod
    def add_date_features(df: DataFrame, date_column: str) -> DataFrame:
        """
        Extract date features from datetime column
        
        Args:
            df: Input DataFrame
            date_column: Name of the date/timestamp column
        """
        logger.info(f"Adding date features from column: {date_column}")
        
        return df.withColumn(f"{date_column}_year", F.year(date_column)) \
                 .withColumn(f"{date_column}_month", F.month(date_column)) \
                 .withColumn(f"{date_column}_day", F.dayofmonth(date_column)) \
                 .withColumn(f"{date_column}_dayofweek", F.dayofweek(date_column)) \
                 .withColumn(f"{date_column}_quarter", F.quarter(date_column)) \
                 .withColumn(f"{date_column}_weekofyear", F.weekofyear(date_column))
    
    @staticmethod
    def create_bins(
        df: DataFrame,
        column: str,
        bins: List[float],
        labels: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Create bins for continuous variable
        
        Args:
            df: Input DataFrame
            column: Column to bin
            bins: List of bin edges
            labels: Optional labels for bins
        """
        logger.info(f"Creating bins for column: {column}")
        
        # Create conditions for binning
        conditions = []
        for i in range(len(bins) - 1):
            condition = (F.col(column) >= bins[i]) & (F.col(column) < bins[i + 1])
            label = labels[i] if labels else f"bin_{i}"
            conditions.append((condition, F.lit(label)))
        
        # Apply conditions
        result = F.when(conditions[0][0], conditions[0][1])
        for condition, label in conditions[1:]:
            result = result.when(condition, label)
        
        return df.withColumn(f"{column}_binned", result)
    
    @staticmethod
    def one_hot_encode(df: DataFrame, column: str) -> DataFrame:
        """
        One-hot encode a categorical column
        
        Args:
            df: Input DataFrame
            column: Column to encode
        """
        logger.info(f"One-hot encoding column: {column}")
        
        # Get distinct values
        distinct_values = [row[column] for row in df.select(column).distinct().collect()]
        
        # Create binary columns
        result_df = df
        for value in distinct_values:
            col_name = f"{column}_{value}".replace(" ", "_")
            result_df = result_df.withColumn(
                col_name,
                F.when(F.col(column) == value, 1).otherwise(0)
            )
        
        return result_df
    
    @staticmethod
    def scale_column(
        df: DataFrame,
        column: str,
        method: str = "minmax"
    ) -> DataFrame:
        """
        Scale numeric column
        
        Args:
            df: Input DataFrame
            column: Column to scale
            method: Scaling method ('minmax' or 'standard')
        """
        logger.info(f"Scaling column {column} using {method}")
        
        if method == "minmax":
            # Min-Max scaling
            min_val = df.agg(F.min(column)).first()[0]
            max_val = df.agg(F.max(column)).first()[0]
            
            return df.withColumn(
                f"{column}_scaled",
                (F.col(column) - min_val) / (max_val - min_val)
            )
        elif method == "standard":
            # Standard scaling (z-score)
            mean_val = df.agg(F.mean(column)).first()[0]
            stddev_val = df.agg(F.stddev(column)).first()[0]
            
            return df.withColumn(
                f"{column}_scaled",
                (F.col(column) - mean_val) / stddev_val
            )
        else:
            raise ValueError(f"Unknown scaling method: {method}")


class JoinTransformer:
    """
    Transformer for joining DataFrames
    
    Example:
        >>> joiner = JoinTransformer()
        >>> df_result = joiner.join(df1, df2, on="customer_id", how="left")
    """
    
    @staticmethod
    def join(
        left_df: DataFrame,
        right_df: DataFrame,
        on: Any,
        how: str = "inner",
        suffix: str = "_right"
    ) -> DataFrame:
        """
        Join two DataFrames
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            on: Join key(s) - string, list, or join condition
            how: Join type (inner, left, right, outer, cross)
            suffix: Suffix for duplicate column names from right DataFrame
        """
        logger.info(f"Joining DataFrames on {on} with {how} join")
        
        # Handle duplicate columns
        right_cols = right_df.columns
        left_cols = left_df.columns
        
        # Rename duplicates in right DataFrame
        rename_dict = {}
        for col in right_cols:
            if col in left_cols and col != on:
                rename_dict[col] = f"{col}{suffix}"
        
        if rename_dict:
            for old_col, new_col in rename_dict.items():
                right_df = right_df.withColumnRenamed(old_col, new_col)
        
        return left_df.join(right_df, on=on, how=how)


class SQLTransformer:
    """
    Transformer for SQL-based transformations
    
    Example:
        >>> sql = SQLTransformer(spark)
        >>> df_result = sql.transform(df, '''
        ...     SELECT category, SUM(amount) as total
        ...     FROM df
        ...     GROUP BY category
        ... ''')
    """
    
    def __init__(self, spark):
        self.spark = spark
    
    def transform(self, df: DataFrame, sql_query: str) -> DataFrame:
        """
        Apply SQL transformation
        
        Args:
            df: Input DataFrame (will be registered as temp view 'df')
            sql_query: SQL query to execute
        """
        logger.info("Applying SQL transformation")
        
        # Register DataFrame as temp view
        df.createOrReplaceTempView("df")
        
        # Execute query
        return self.spark.sql(sql_query)


# Export all transformers
__all__ = [
    "SparkTransformer",
    "DataCleaningTransformer",
    "FeatureEngineeringTransformer",
    "JoinTransformer",
    "SQLTransformer"
]
