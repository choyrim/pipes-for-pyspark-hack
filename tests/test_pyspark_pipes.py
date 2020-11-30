"""
test spark sql functions
"""
# pylint: disable=unused-wildcard-import,wildcard-import,redefined-outer-name

import pytest

import pandas as pd

import pyspark_pipes as pp
from pyspark_pipes import to_pandas

def test_that_spark_starts(spark):
    df = spark.sql("show databases")
    df.show()
    assert df | pp.count == 1

def test_first_database(spark):
    df = spark.sql("show databases")
    df.show()
    assert df.first()["namespace"] == "default"

def test_count(sample_df):
    n = sample_df | pp.count
    assert n == 1000

def test_select(sample_df):
    df2 = sample_df | pp.select("Region", "Country")
    assert df2.columns == ["Region", "Country"]

def test_where(sample_df):
    n = sample_df | pp.where("Region = 'North America'") | pp.count
    assert n == 19

def test_group_by(sample_df):
    n = sample_df | pp.group_by("Region") | pp.agg("count(1) n") | pp.count
    assert n == 7

def test_order_by(sample_df):
    df2 = sample_df | pp.order_by("Order ID")
    r = df2.first()
    assert r["Order ID"] == 102928006
    assert r["Region"] == "Asia"
    assert r["Country"] == "India"

def test_limit(sample_df):
    n = sample_df | pp.limit(3) | pp.count
    assert n == 3

def test_drop(sample_df):
    df2 = sample_df | pp.drop("Region")
    assert "Region" in sample_df.columns
    assert "Region" not in df2.columns

def test_repartition(sample_df):
    df2 = sample_df | pp.repartition(10)
    assert sample_df.rdd.getNumPartitions() == 1
    assert df2.rdd.getNumPartitions() == 10

def test_to_pandas(sample_df):
    pdf = sample_df | pp.to_pandas
    assert isinstance(pdf, pd.DataFrame)
