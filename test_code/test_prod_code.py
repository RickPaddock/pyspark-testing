# Importing the pyspark functions and setting up test functions
import sys
import os

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))

# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)

# adding the parent directory to
# the sys.path.
sys.path.append(parent)

from prod_code import do_the_agg, do_the_other_agg
from confest import spark


def get_data(spark):
    """Set up test data"""
    data = [
        {"id": 1, "name": "abc1", "value": 22},
        {"id": 2, "name": "abc1", "value": 23},
        {"id": 3, "name": "def2", "value": 33},
        {"id": 4, "name": "def2", "value": 44},
        {"id": 5, "name": "def2", "value": 55},
    ]
    df = spark.createDataFrame(data).coalesce(1)
    return df


def test_can_agg(spark):
    df = get_data(spark)
    df_agg = do_the_agg(df)

    assert "sumval" in df_agg.columns

    out = df_agg.sort("name", "sumval").collect()

    assert len(out) == 2
    assert out[0]["name"] == "abc1"
    assert out[1]["sumval"] == 132


def test_can_do_other_agg(spark):
    df = get_data(spark)
    df_agg = do_the_other_agg(df)

    assert "maxval" in df_agg.columns

    out = df_agg.sort("name", "maxval").collect()

    assert len(out) == 2
    assert out[0]["name"] == "abc1"
    assert out[1]["maxval"] == 55
