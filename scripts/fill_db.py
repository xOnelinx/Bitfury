#!/usr/bin/python3
# -*- coding: utf-8 -*-
import os
import random

import psycopg2


def get_unique_pairs(seq_len):
    """ Create set of unique id pairs"""
    pairs = set()
    while len(pairs) < seq_len:
        pairs.add((random.randint(1, 1000), random.randint(1, 1000)))
    return pairs


def get_insert_query(rows_num):
    """ Create insert query with random data """
    rows = []
    unique_pairs = get_unique_pairs(rows_num)
    for user_id, event_id in unique_pairs:
        amount = random.randint(-100000, 100000)
        rows.append(
            f'insert into public.raw_data (user_id, event_id, amount) values ({user_id}, {event_id}, {amount} );')

    return ''.join(rows)


def main():
    """
    Задание:
    1) Заполнение базы сырыми данными (таблица raw_data). Сырые данные генерируются случайным образом.
    Если таблица уже заполнена данными, то скрипт просто выходит.
    Сгенерировать надо случайное число строк между 90000 и 100000.
    При этом параметры user_id и event_id могут принимать значения от 1 до 1000, а amount от -100000 до 100000.
    Пара (user_id,event_id) должна быть уникальная.

    raw_data (
       id int auto increment primary key,
       user_id unsigned int,
       event_id unsigned int,
       amount int
    )

    """

    host = os.getenv('DB_HOST', 'localhost')
    db = 'test'
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')

    connection = psycopg2.connect(host=host, database=db, user=user, password=password)
    cursor = connection.cursor()

    count_query = 'select count(*) from public.raw_data'
    cursor.execute(count_query)
    row_count = cursor.fetchone()[0]
    if not row_count:
        cursor.execute(get_insert_query(random.randint(90000, 100000)))
        connection.commit()
        cursor.execute(count_query)
        row_count = cursor.fetchone()[0]
        print(f'{row_count} rows created in table raw_data')
    else:
        print('raw_data is full')
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
