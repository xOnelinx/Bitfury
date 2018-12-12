# Bitfury
test task from Bitfury

**Необходимо написать три скрипта**
1) Заполнение базы сырыми данными (таблица raw_data). Сырые данные генерируются случайным образом. Если таблица уже заполнена данными, то скрипт просто выходит. Сгенерировать надо случайное число строк между 90000 и 100000. При этом параметры user_id и event_id могут принимать значения от 1 до 1000, а amount от -100000 до 100000. Пара (user_id,event_id) должна быть уникальная.
2) Скрипт-аггрегатор. Скрипт за один запуск должен вытаскивать не более 10000 строк из базы в сыром виде, обсчитывать необходимые значения в памяти (т.е. без агрегирующих функций на стороне базы) и обновлять данные в agg_data по каждому из юзеров. После прохода он должен писать последний обработанный id в last_processed_id, и при следующем запуске начинать с этого id. Необходимо запустить скрипт столько раз, сколько потребуется для прохода по всем данным raw_data



*to run all scripts:*
* $ pip install -r requerements.txt
* $ make create_db
* wait few seconds while db up
* $ make fill_db
* $ make calc 

*Optional, may be added env variables, to use another db:*
```
DB_HOST - db host, default: 'localhost'
DB - db name default: 'test'
POSTGRES_USER - db user, default: 'postgres'
POSTGRES_PASSWORD - users password default: 'postgres'
DB_PORT - db port, default: 5432
```

**To use another db, create a tables in it:**

```
CREATE SEQUENCE public.raw_data_id_seq NO MINVALUE NO MAXVALUE NO CYCLE;

CREATE TABLE raw_data (
  id       int primary key default nextval('raw_data_id_seq'::regclass),
  user_id  int NOT NULL,
  event_id int NOT NULL,
  amount   int,
  unique (user_id, event_id)
);

CREATE TABLE agg_data (
  user_id        int primary key,
  balance        int,
  event_number   int,
  best_event_id  int,
  worst_event_id int
);

CREATE TABLE last_processed_id (
  id int
);
```

*to delete db with docker container:*
* $ make delete_db

*help:*
* $ make help