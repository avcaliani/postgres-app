from argparse import ArgumentParser

import pandas as pd
from sqlalchemy import create_engine

from src.util import Timer

TABLE_NAME = 'user_score'


def get_args():
    parser = ArgumentParser(description='Postgres App')
    parser.add_argument('-p', dest='pipeline', required=True, help='Pipeline')
    parser.add_argument('-f', dest='file', default='./data/user-score.csv', help='Pipeline')
    parser.add_argument('--overwrite', nargs='?', type=bool, const=True, default=False, help='Overwrite data?')
    return parser.parse_args()


def read_file(file_path):
    print('=~=~=~=~=~=~=~= READ =~=~=~=~=~=~=~=')
    print(f'File: {file_path}')
    df = pd.read_csv(file_path)
    print(f'Lines: {df.shape[0]}')
    print(f'Columns: {df.shape[1]}')
    df.info(verbose=True)
    return df


def credentials():
    db_user = 'app-user'
    db_password = 'pass4app'
    db_host = 'localhost'
    db_name = 'postgres-app'
    return f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}'


def report():
    print('=~=~=~=~=~=~= READ SQL =~=~=~=~=~=~=')
    db = create_engine(credentials()).connect()
    df = pd.read_sql(
        sql="""
        select 
            count(*) as records,
            count(distinct document) as users 
        from "postgres-app".public.user_score
        """,
        con=db
    )
    print(df)


def save(df, overwrite):
    db = create_engine(credentials()).connect()
    df.to_sql(
        name=TABLE_NAME,
        con=db,
        if_exists='replace' if overwrite else 'append',
        index=False
    )


if __name__ == '__main__':
    args, timer = get_args(), Timer()
    pipeline = str(args.pipeline).lower().strip()
    if pipeline == 'report':
        report()
    elif pipeline == 'save':
        save(df=read_file(args.file), overwrite=args.overwrite)
    else:
        raise RuntimeError(f'Pipeline "{args.pipeline}" does not exist.')
    print(f'Elapsed time: {timer.stop()}')
