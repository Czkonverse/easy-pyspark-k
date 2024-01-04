from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as PySparkDF


# noinspection SqlNoDataSourceInspection
def get_data_from_hive(spark: SparkSession, input_date: str) -> PySparkDF:
    """
    This function reads data from a Hive table using PySpark.

    Parameters:
    spark (SparkSession): The SparkSession object to execute Spark operations.
    input_date (str): The date to filter the data on the 'inc_day' column.

    Returns:
    PySparkDF: A DataFrame containing the data read from the Hive table.

    Note:
    The SQL query used to read the data does not specify a data source.
    """

    # SQL query to read data from a Hive table
    # The table is filtered based on the 'inc_day' column
    exe_sql = f"""
    SELECT
        *
    FROM db_name.table_name
    WHERE 1=1
        AND inc_day='{input_date}'
    """

    # Execute the SQL query using the SparkSession and store the result in a DataFrame
    df = spark.sql(exe_sql)

    # Return the DataFrame
    return df
