'''
read file from /datas folder, and print the first 5 rows
'''

import pandas as pd
from functools import reduce


def remove_space(string: str):
    '''remove all space in string'''
    return string.replace(" ", "")


def concat(string1: str, string2: str):
    '''concat two strings'''
    return string1 + string2


# set pandas to display all columns and rows
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option("display.max_colwidth", None)

def create_hash_by_columns(df: pd.DataFrame, columns: list[str]):
    return reduce(concat, [
        remove_space(df[column].str) for column in columns
    ])


def get_duplicated_rows_by_columns(df: pd.DataFrame, columns: list[str]):
    '''get all duplicated rows by columns'''

    # create a new column 'hash'
    df['hash'] = create_hash_by_columns(df, columns)

    # get duplicated rows
    dup = df[df.duplicated('hash')]

    # remove column 'hash'
    dup = dup.drop(columns=['hash'])

    return dup


def deduplicate_by_columns(df: pd.DataFrame, columns: list[str]):
    '''deduplicate all rows by columns'''

    # create a new column 'hash'
    df['hash'] = create_hash_by_columns(df, columns)

    # drop duplicated rows by column 'hash'
    dedup = df.drop_duplicates(subset='hash')

    # remove column 'hash'
    dedup = dedup.drop(columns=['hash'])

    return dedup


def task1(files: list[str]):
    print('''
    ============================ Task1 Start ==================================
    how many rows are duplicated by column 'Annotation' and 'Citation Neutral'
    ''')

    for file in files:
        rows = get_duplicated_rows_by_columns(
            pd.read_csv(file),
            ['Annotation', 'Citation Neutral']
        )
        print(f'File: {file}')
        print(f'Count: {len(rows)}')
        print(rows)

    print('''
    ============================ Task1 End ====================================
    ''')


def task2(files: list[str]):
    print('''
    ============================ Task2 Start ==================================
    deduplicate all rows by column 'Annotation' and 'Citation Neutral'
    ''')

    for file in files:
        rows = deduplicate_by_columns(
            pd.read_csv(file),
            ['Annotation', 'Citation Neutral']
        )
        print(f'File: {file}')
        print(f'Count: {len(rows)}')
        print(rows)

    print('''
    ============================ Task2 End ====================================
    ''')


def main(files: list[str]):
    total_by_files = 0
    dup_by_files = 0
    dedup_by_files = 0

    for file in files:
        print(f'File: {file}')

        file = pd.read_csv(file)
        dup = get_duplicated_rows_by_columns(
            file,
            ['Annotation', 'Citation Neutral']
        )
        dedup = deduplicate_by_columns(
            file,
            ['Annotation', 'Citation Neutral']
        )

        total_should_be_equal = len(file) == len(dup) + len(dedup)
        assert total_should_be_equal, 'deduplicated rows + duplicated rows != total rows'

        print(f'Total Rows: {len(file)}')
        print(f'Duplicated Rows: {len(dup)}')
        print(f'Deduplicated Rows: {len(dedup)}')
        print()

        # side effect
        total_by_files += len(file)
        dup_by_files += len(dup)
        dedup_by_files += len(dedup)

    files = pd.concat([
        pd.read_csv(file) for file in files
    ])
    dup = get_duplicated_rows_by_columns(
        files,
        ['Annotation', 'Citation Neutral']
    )
    dedup = deduplicate_by_columns(
        files,
        ['Annotation', 'Citation Neutral']
    )

    assert total_by_files == len(
        files), 'total rows by files != total rows by concat'
    # assert dup_by_files == len(
    #     dup), 'duplicated rows by files != duplicated rows by concat'
    # assert dedup_by_files == len(
    #     dedup), 'deduplicated rows by files != deduplicated rows by concat'

    total_should_be_equal = len(files) == len(dup) + len(dedup)
    assert total_should_be_equal, 'deduplicated rows + duplicated rows != total rows'

    print('All Files')
    print(f'Total Rows: {len(files)}')
    print(f'Duplicated Rows: {len(dup)}')
    print(f'Deduplicated Rows: {len(dedup)}')


if __name__ == "__main__":
    files = [
        'datas/05_04.csv',
        'datas/05_11.csv',
        'datas/05_18.csv',
        'datas/05_22.csv',
    ]
    main(files)
