#!/usr/bin/python3
# -*- coding: utf-8 -*-
import os

from sqlalchemy import text, create_engine
import pandas


def get_raw_data(connection):
    first_id = get_last_processed_id(connection)

    query = f'select * from public.raw_data where id >= {first_id} and id < {first_id + 10000};'
    df = pandas.read_sql(query, connection)
    return df


def write_to_agg_data(data, connection):
    query = 'select * from public.agg_data;'
    old_df = pandas.read_sql(query, connection)

    if old_df.empty:
        data.to_sql('agg_data', connection, if_exists='replace')
    else:
        old_df = old_df.join(data, on='user_id', rsuffix='_new')
        old_df['balance'] = old_df['balance_new'] + old_df['balance']
        old_df['event_number'] = old_df['event_number_new'] + old_df['event_number']
        old_df['best_event_id'] = old_df[['best_event_id_new', 'best_event_id']].max(axis=1)
        old_df['worst_event_id'] = old_df[['worst_event_id_new', 'worst_event_id']].min(axis=1)
        new_data = old_df[['balance', 'event_number', 'best_event_id', 'worst_event_id']]
        new_data.index = old_df['user_id']

        new_data.to_sql('agg_data', connection, if_exists='replace')


def write_to_last_processed_id(last_id, connection):
    connection.execute(text(f'insert into public.last_processed_id values ({last_id});'))
    print('last_id', last_id)


def get_last_processed_id(connection):
    """ If no last id from last_processed_id, return first id from raw_data """
    query = 'select id from last_processed_id;'
    res = connection.execute(text(query)).fetchall()
    if res:
        first_id = res[-1][0]
    else:
        query = 'select id from public.raw_data limit 1'
        res = connection.execute(text(query)).fetchall()
        if not res:
            raise Exception('Tables last_processed_id and raw_data is empty, run fill_db.py !')
        first_id = res[0][0]

    print('first_id', first_id)
    return first_id


def main():
    """
    Задание:
    В таблице agg_data собираются статистические данные по каждому юзеру:
    agg_data (
       user_id unsigned int primary key,
       balance int, - сумма amount по всем ивентам,
       event_number unsigned int, - количество ивентов, в которых юзер участвовал,
       best_event_id unsigned int, - ID ивента в котором у юзера максимальный amount,
       worst_event_id unsigned int - ID ивента в котором у юзера минимальный amount.
    )

    last_processed_id (
       id number
    )
    2) Скрипт-аггрегатор. Скрипт за один запуск должен вытаскивать не более 10000 строк из базы в сыром виде,
     обсчитывать необходимые значения в памяти (т.е. без агрегирующих функций на стороне базы)
     и обновлять данные в agg_data по каждому из юзеров.
     После прохода он должен писать последний обработанный id в last_processed_id,
     и при следующем запуске начинать с этого id. Необходимо запустить скрипт столько раз,
     сколько потребуется для прохода по всем данным raw_data
    """

    HOST = os.getenv('DB_HOST', 'localhost')
    DB = 'test'
    USER = os.getenv('POSTGRES_USER', 'postgres')
    PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
    PORT = os.getenv('DB_PORT', 5432)

    url = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}'
    engine = create_engine(url)

    df = get_raw_data(engine)

    event_number_ser = df.groupby(['user_id'])['event_id'].count()
    balance_ser = df.groupby(['user_id'])['amount'].sum()
    best_event_id_ser = df.groupby(['user_id'])['amount'].max()
    worst_event_id_ser = df.groupby(['user_id'])['amount'].min()

    result_df = pandas.DataFrame(data={
        'event_number': event_number_ser,
        'balance': balance_ser,
        'best_event_id': best_event_id_ser,
        'worst_event_id': worst_event_id_ser,

    })

    last_id = df['id'][df.index[-1]]
    write_to_last_processed_id(last_id, engine)
    write_to_agg_data(result_df, engine)


if __name__ == "__main__":
    main()
