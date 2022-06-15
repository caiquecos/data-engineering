CREATE DATABASE analytics;

CREATE TABLE IF NOT EXISTS sales_of_oil_derivative_fuels (
        combustivel	varchar,
        ano	varchar,
        regiao	varchar,
        estado	varchar,
        jan	numeric(10,2),
        fev	numeric(10,2),
        mar	numeric(10,2),
        abr	numeric(10,2),
        mai	numeric(10,2),
        jun	numeric(10,2),
        jul	numeric(10,2),
        ago	numeric(10,2),
        set	numeric(10,2),
        out	numeric(10,2),
        nov	numeric(10,2),
        dez	numeric(10,2),
        total numeric(10,2)
        );

CREATE TABLE IF NOT EXISTS sales_of_diesel (
        combustivel	varchar,
        ano	varchar,
        regiao	varchar,
        estado	varchar,
        jan	numeric(10,2),
        fev	numeric(10,2),
        mar	numeric(10,2),
        abr	numeric(10,2),
        mai	numeric(10,2),
        jun	numeric(10,2),
        jul	numeric(10,2),
        ago	numeric(10,2),
        set	numeric(10,2),
        out	numeric(10,2),
        nov	numeric(10,2),
        dez	numeric(10,2),
        total numeric(10,2)
        );

CREATE TABLE IF NOT EXISTS raw_sales_of_oil_derivative_fuels(
    year_month	date,
    uf	varchar,
    product	varchar,
    unit	varchar,
    volume	numeric(10,2),
    created_at	timestamp
);

CREATE INDEX idx_crsodf_created_at
ON raw_sales_of_oil_derivative_fuels(created_at);

CREATE TABLE IF NOT EXISTS raw_sales_of_diesel(
    year_month	date,
    uf	varchar,
    product	varchar,
    unit	varchar,
    volume	numeric(10,2),
    created_at	timestamp 
);

CREATE INDEX idx_sod_created_at
ON raw_sales_of_diesel(created_at);
