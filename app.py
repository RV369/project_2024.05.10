import clickhouse_connect
import pandas as pd


def write_pandas_df():
    client = clickhouse_connect.get_client(
        host='localhost', port='8123', user='default', password='',
    )
    client.command('DROP TABLE IF EXISTS pandas_example')
    create_table_sql = """CREATE TABLE pandas_example
    (`username` String,`ipv4` String,`mac` String)
    ENGINE = MergeTree ORDER BY username"""
    client.command(create_table_sql)
    df = pd.DataFrame(
        {
            'username': ['username1', 'username2', 'username3'],
            'ipv4': ['192.168.0.1', '192.168.0.2', '192.168.0.3'],
            'mac': [
                '01:23:45:67:89:ab',
                '02:34:56:78:90:cd',
                '02:88:56:78:98:sk',
            ],
        },
    )
    client.insert_df('pandas_example', df)
    result_df = client.query_df('SELECT * FROM pandas_example')
    print()
    print(result_df.dtypes)
    print()
    print(result_df)


def search(column, obj):
    client = clickhouse_connect.get_client(
        host='localhost', port='8123', user='default', password='',
    )
    if column == 'username':
        search = f"SELECT * FROM pandas_example WHERE username = '{obj}'"
    elif column == 'ipv4':
        search = f"SELECT * FROM pandas_example WHERE ipv4 = '{obj}'"
    elif column == 'mac':
        search = f"SELECT * FROM pandas_example WHERE mac = '{obj}'"
    result_df = client.command(search)
    print('search>>>>', *result_df)


if __name__ == '__main__':
    write_pandas_df()
    print()
    search(str('username'), str('username2'))
    search(str('ipv4'), str('192.168.0.1'))
    search(str('mac'), str('02:88:56:78:98:sk'))
