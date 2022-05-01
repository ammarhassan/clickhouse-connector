import traceback

from clickhouse_driver import Client
import pandas as pd
from datetime import datetime

PANDAS_TO_CH_TYPES = {
    'int64': 'Int64',
    'float64': 'Float64'
}


def get_create_clickhouse_table_query(table_name, column_names,
                                      primary_key, column_data_types: dict = None):
    """
    This function creates a datatable
    :param table_name: name of the table in CH
    :param column_names: column names in the table
    :param primary_key: primary key, look up CH primary keys as they are a diff. concept
    :param column_data_types: The CH data types for each column
    :return: a query that runs directly in CH and creates a table with correct data types
    """

    column_data_types = column_data_types or {}
    # fill up column data types
    column_data_types = dict([(column, column_data_types.get(column, 'String')) for column in column_names])

    query = f"Create Table {table_name} ( "
    query += ', '.join([f"{column} {column_data_types[column]}" +
                        # primary key can not be nullable
                        ("" if column == primary_key else " NULL")
                        for column in column_names])
    query += f") ENGINE = MergeTree() "
    if primary_key:
        query += f" Primary Key ({primary_key},)"
    return query


def get_drop_table_query(table_name: str) -> str:
    return f"DROP TABLE IF EXISTS {table_name}"


def drop_create_clickhouse_table(ch_client, create_table_query: str, drop_table_query: str) -> bool:
    ch_client.execute(drop_table_query)
    ch_client.execute(create_table_query)
    return True


client = Client(host='127.0.0.1')
table_name = "vidyocdr"
primary_key = 'join_time'
file = "data/vidyocdr_3m.csv"

df_iterator = pd.read_csv(
    file,
    chunksize=100000, parse_dates=True, infer_datetime_format=True,
    low_memory=False)

t3 = datetime.now()
for i, df_chunk in enumerate(df_iterator):
    t1 = datetime.now()
    print(f'Time Spend reading chunk {i}: ', t1 - t3)
    if i == 0:
        # get pandas data types
        pandas_data_types = dict([(k, str(v)) for k, v in dict(df_chunk.dtypes).items()])
        # use explicit string types instead of object, as object is a mixed type and would not convert
        # the other types in the column to a string, causing issues later as data gets written to the
        # CH database
        pandas_data_types = dict([(k, 'str' if v == 'object' else v) for k, v in pandas_data_types.items()])

        print(pandas_data_types)
        ch_data_types = dict([
            (column, PANDAS_TO_CH_TYPES.get(p_type, 'String'))
            for column, p_type in pandas_data_types.items()])

        # Create table
        table_query = get_create_clickhouse_table_query(
            table_name, list(df_chunk.columns),
            primary_key, ch_data_types)

        drop_table_query = get_drop_table_query(table_name)
        drop_create_clickhouse_table(client, table_query, drop_table_query)

    # fill out N.As in the data
    df_chunk.fillna(df_chunk.dtypes.replace({'float64': 0.0, 'O': 'NULL', 'int64': 0}),
                    downcast='infer', inplace=True)

    # apply data types to pandas dataframe from the constructed table data types
    df_chunk = df_chunk.astype(pandas_data_types)

    t2 = datetime.now()
    try:
        client.execute(f"INSERT INTO {table_name} VALUES", df_chunk.to_dict('records'))
    except Exception as e:
        print(traceback.format_exc())
        exit()

    print(f'Done writing chunk, written in ', datetime.now() - t2)
    t3 = datetime.now()
