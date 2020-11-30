"""
test fixtures
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

this_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(this_dir, "../src"))
print(f"src_dir={src_dir}")
sys.path.insert(0, src_dir)

# for pytest fixtures
# pylint: disable=redefined-outer-name

@pytest.fixture(scope="session")
def spark(request):
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName(f"pytest:{request.node.fspath}")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )
    def tear_down():
        session.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown (copied from sparktestingbase.sqltestcase.SQLTestCase)
        # pylint: disable=protected-access
        session._jvm.System.clearProperty("spark.driver.port")
    request.addfinalizer(tear_down)
    return session

@pytest.fixture
def sample_df(spark):
    df = spark.read.csv(
        os.path.join(this_dir, "sample-data.csv"),
        header=True,
        inferSchema=True,
    )
    return df
