#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    DROP DATABASE IF EXISTS test;
    CREATE DATABASE test;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d test <<-EOSQL
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

EOSQL
