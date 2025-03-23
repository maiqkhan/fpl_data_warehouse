CREATE SCHEMA IF NOT EXISTS fpl;
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE stg.dates (
    date_key INTEGER PRIMARY KEY NOT NULL,
    date_id date NOT NULL,
    year INTEGER NOT NULL,
    month_num INTEGER NOT NULL,
    month_name VARCHAR(9) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(9) NOT NULL
);