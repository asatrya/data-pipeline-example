USE `db`;

CREATE TABLE voucher_recency_most_used (
    execution_date VARCHAR(20) NOT NULL,
    country_code VARCHAR(30) NOT NULL,
    recency_segment VARCHAR(10) NOT NULL,
    voucher_amount INT(6) NOT NULL
);

CREATE TABLE voucher_frequent_most_used (
    execution_date VARCHAR(20) NOT NULL,
    country_code VARCHAR(30) NOT NULL,
    frequent_segment VARCHAR(10) NOT NULL,
    voucher_amount INT(6) NOT NULL
);