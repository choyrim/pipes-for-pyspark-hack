"""
pipe operator for pyspark (hack)
"""
import inspect
import logging
import re
import pathlib
from typing import Callable

import pyspark.sql
import pyspark.sql.dataframe
import pyspark.sql.functions as F

def _hack_pipe(self, func: Callable):
    return func(self)

pyspark.sql.dataframe.DataFrame.__or__ = _hack_pipe
pyspark.sql.GroupedData.__or__ = _hack_pipe

def show(top=10):
    return lambda df: df.limit(top).toPandas()

def explain(df):
    return df.explain()

def where(condition):
    return lambda df: df.where(condition)

def select(*exprs):
    return lambda df: df.selectExpr(*exprs)

def count(df):
    return df.count()

def order_by(*cols, **kwargs):
    return lambda df: df.orderBy(*cols, **kwargs)

def repartition(*cols):
    return lambda df: df.repartition(*cols)

def group_by(*cols):
    return lambda df: df.groupBy(*cols)

def agg(*exprs):
    return lambda df: df.agg(*[F.expr(s) for s in exprs])

def join(other, on=None, how=None):
    on_expr = F.expr(on) if on else None
    return lambda df: df.join(other, on=on_expr, how=how)

def drop(*cols):
    return lambda df: df.drop(*cols)

def alias(name):
    return lambda df: df.alias(name)

def limit(n=10):
    return lambda df: df.limit(n)

def to_pandas(df):
    return df.toPandas()
