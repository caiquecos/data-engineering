CREATE TABLE IF NOT EXISTS raw_sales_of_oil_derivative_fuels(
    year_month	date,
    uf	varchar,
    product	varchar,
    unit	varchar,
    volume	numeric(10,2),
    created_at	timestamp
)