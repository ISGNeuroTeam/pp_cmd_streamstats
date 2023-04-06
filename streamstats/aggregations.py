from typing import Union

import pandas as pd


def _sum(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _stddev(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _max(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _min(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _avg(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _count(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _distinct_count(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _var(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _first(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _last(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _list(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _values(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _earliest(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


def _latest(df: pd.DataFrame, window: Union[int, str], args, grouped_by):
    ...


aggregations = {
    'avg': _avg,
    'count': _count,
    'distinct_count': _distinct_count,
    'min': _min,
    'max': _max,
    'stddev': _stddev,
    'sum': _sum,
    'var': _var,
    'first': _first,
    'last': _last,
    'list': _list,
    'values': _values,
    'earliest': _earliest,
    'latest': _latest
}


def generate(dataframe: pd.DataFrame, name: str, named_as: str, window: Union[int, str], **params):
    fn = aggregations[name]
    return pd.DataFrame(fn(dataframe, window=window, **params), columns=[named_as])
