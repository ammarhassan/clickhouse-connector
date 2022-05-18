import re
import traceback

from clickhouse_driver import Client
import pandas as pd
from datetime import datetime

PANDAS_TO_CH_TYPES = {
    'int64': 'Int64',
    'float64': 'Float64',
    'datetime64[ns]': 'DateTime',
    'datetime': 'DateTime',
    'datetime64': 'DateTime'
}


def dt_inplace(df):
    """Automatically detect and convert (in place!) each
    dataframe column of datatype 'object' to a datetime just
    when ALL of its non-NaN values can be successfully parsed
    by pd.to_datetime().  Also returns a ref. to df for
    convenient use in an expression.
    """
    from pandas.errors import ParserError
    for c in df.columns[df.dtypes == 'object']:  # don't count ints and floats
        try:
            df[c] = pd.to_datetime(df[c])
        except (ParserError, ValueError, TypeError, OverflowError):  # Can't convert some of the columns
            pass  # ...so leave whole column as-is unconverted
    return df


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


def to_ascii(column_name):
    c_name = re.sub(r'\W+', '_', column_name)
    c_name = str(c_name).lower()
    return c_name


def treat_column_names(column_names, alteration_function=None):
    """
    This function updates the name of the columns of the input dataframe according to the
    alteration function.
    :param column_names:
    :param alteration_function:
    :return:
    """
    alteration_function = alteration_function or to_ascii
    print('column names are', column_names)
    column_names_dict = {}

    for index, column in enumerate(column_names):
        altered_column = alteration_function(column)
        if len(altered_column) == 0:
            altered_column = f"column_{index}"
        elif altered_column[0].isdigit():
            altered_column += f"column_{index}_{altered_column}"
        column_names_dict[column] = altered_column
    return column_names_dict


client = Client(host='127.0.0.1', database='trp', user='trp_user', password='trppassword')

table_name = "zoommeetings"
primary_key = 'Start Time'
file = "data/ignishealth_Zoom_meetings.csv"

batch_size = 100000
gt1 = datetime.now()

df_iterator = pd.read_csv(
    file,
    chunksize=batch_size, parse_dates=True, infer_datetime_format=True,
    low_memory=False)

t3 = datetime.now()
for i, df_chunk in enumerate(df_iterator):
    t1 = datetime.now()
    df_chunk = dt_inplace(df_chunk)
    # print(df_chunk.dtypes)
    print(f'Time Spend reading chunk {i}: ', t1 - t3)
    columns_rename_dict = treat_column_names(df_chunk.columns, to_ascii)
    df_chunk = df_chunk.rename(columns=columns_rename_dict)

    if i == 0:
        # get pandas data types
        pandas_data_types = dict([(k, str(v)) for k, v in dict(df_chunk.dtypes).items()])
        # use explicit string types instead of object, as object is a mixed type and would not convert
        # the other types in the column to a string, causing issues later as data gets written to the
        # CH database
        pandas_data_types = dict([(k, 'str' if v == 'object' else v) for k, v in pandas_data_types.items()])

        # print(pandas_data_types)
        ch_data_types = dict([
            (column, PANDAS_TO_CH_TYPES.get(p_type, 'String'))
            for column, p_type in pandas_data_types.items()])

        # Create table
        table_query = get_create_clickhouse_table_query(
            table_name, list(df_chunk.columns),
            columns_rename_dict[primary_key], ch_data_types)

        drop_table_query = get_drop_table_query(table_name)
        drop_create_clickhouse_table(client, table_query, drop_table_query)

    # print('before filling na', df_chunk.dtypes)
    # fill out N.As in the data
    df_chunk.fillna(df_chunk.dtypes.replace({'float64': 0.0, 'O': 'NULL', 'int64': 0, 'datetime64[ns]': '1970-01-01'}),
                    downcast='infer', inplace=True)
    # print(df_chunk['end_time'])

    # apply data types to pandas dataframe from the constructed table data types
    # print('pandas data types', pandas_data_types)
    # print('current dtypes', df_chunk.dtypes)
    df_chunk = df_chunk.astype(pandas_data_types)

    t2 = datetime.now()
    try:
        client.execute(f"INSERT INTO {table_name} VALUES", df_chunk.to_dict('records'))
    except Exception as e:
        print(traceback.format_exc())
        exit()

    print(f'Done writing chunk, written in ', datetime.now() - t2)
    t3 = datetime.now()

print(f'Total time taken for {batch_size}: ', datetime.now() - gt1)
