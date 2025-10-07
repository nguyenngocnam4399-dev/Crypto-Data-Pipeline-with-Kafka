CREATE TABLE symbol_dim_1 (
    symbol_id INT AUTO_INCREMENT PRIMARY KEY,
    symbol_name VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE interval_dim_1 (
    interval_id INT AUTO_INCREMENT PRIMARY KEY,
    interval_name VARCHAR(10) NOT NULL UNIQUE
);

CREATE TABLE kline_fact_1 (
    kline_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol_id INT NOT NULL,
    interval_id INT NOT NULL,
    open_price DECIMAL(20,10) NOT NULL,
    high_price DECIMAL(20,10) NOT NULL,
    low_price DECIMAL(20,10) NOT NULL,
    close_price DECIMAL(20,10) NOT NULL,
    volume DECIMAL(38,18) NOT NULL,
    open_time DATETIME NOT NULL,
    close_time DATETIME NOT NULL,

	FOREIGN KEY (symbol_id) REFERENCES symbol_dim(symbol_id),
    FOREIGN KEY (interval_id) REFERENCES interval_dim(interval_id),
    CONSTRAINT unique_kline UNIQUE(symbol_id, interval_id, open_time)
);
