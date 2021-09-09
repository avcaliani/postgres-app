""" Script to generate mocked records. """
import re
from datetime import date, timedelta
from os import makedirs
from random import randint

import pandas as pd
from faker import Faker
from validate_docbr import CPF


def get_users(how_many):
    """Return a list of base users.

    This method will return a number (how_many) of users with basic fields like:
        - Name
        - Email
        - Document

    :param how_many: How many users.
    :return: List of users.
    """
    users = []
    fake = Faker(['en_US', 'pt_BR'])
    docs = CPF().generate_list(n=how_many, mask=False, repeat=False)
    for i in range(how_many):
        name = re.sub(r'^(Dr|Ms|Sr|Sra)\.\s', '', fake.name()).strip()
        users.append({
            'name': name,
            'email': f'{name.lower().replace(" ", ".")}@{fake.free_email_domain()}',
            'document': docs[i],
        })
    return users


def gen_score(user, updated_at):
    """Generate random score to a specific user.

    This method will not replace the income user, it will create another one.

    :param user: Base user.
    :param updated_at: Record date.
    :return: User with score and date.
    """
    user_ = user.copy()
    user_['score'] = randint(0, 10)
    user_['updated_at'] = updated_at
    return user_


def save(df, out_path):
    """Write mock data as CSV.

    :param df: Pandas Data Frame.
    :param out_path: Output Path.
    """
    print('------------< save >------------')
    out_file = 'user-score.csv'
    makedirs(out_path, exist_ok=True)
    print(f'path: {out_path}/{out_file}')
    print(f'shape: {df.shape}')
    df.to_csv(f'{out_path}/{out_file}', index=False)
    print('--------------------------------')


def create(output, users, period):
    """Create user score time series dataset.

    :param output: Output path.
    :param users: How many users.
    :param period: Time series period.
    """
    now = date.today()
    begin = now - timedelta(days=period)
    mock, mock_users = [], get_users(users)
    while begin < now:
        mock.extend([gen_score(user, begin) for user in mock_users])
        begin += timedelta(days=1)

    df = pd.DataFrame(mock)
    df = df.sort_values(['document', 'updated_at'])
    save(df, output)


if __name__ == '__main__':
    print('Creating user scores...')
    create('./data', users=10, period=120)
    print('#done')
