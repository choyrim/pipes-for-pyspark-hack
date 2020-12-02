# Pipe Operator for Pyspark

This module provides a hack for pyspark to allow
composing dataframe operations with a pipe operator
and some custom functions.

**example usage:**

```python
import pyspark_pipes as pp

df = spark.read.csv("sample.csv", header=True, inferSchema=True)
n = (df
    | pp.where("Region like 'North%'")
    | pp.count)
```

The above is equivalent to the following method chain:

```python
m = (df
    .where("Region like 'North%'")
    .count())
```

The composition with pipes becomes more useful when
operations are named.

```python
def northern(df):
    return df | pp.where("Region like 'North%'")

df2 = df | northern | pp.count
```
