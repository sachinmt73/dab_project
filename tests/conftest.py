import os
import sys
import pytest

sys.path.append(os.getcwd())  # Adjust the path as necessary to import from src


@pytest.fixture()
def spark():
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.remote(cluster_id="1231-131053-qw49jkds").getOrCreate()
    except ImportError:
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
        except ImportError:
            raise ImportError("No suitable Spark session found. Please install Databricks Connect or PySpark.")
    return spark
