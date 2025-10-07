CREATE TABLE dim_indicator_type_1 (
    type_id INT AUTO_INCREMENT PRIMARY KEY,
    type_name VARCHAR(20) UNIQUE NOT NULL
);

-- Chèn dữ liệu cố định
INSERT INTO dim_indicator_type_1 (type_name)
VALUES ('SMA'), ('RSI'), ('BB_UP'), ('BB_DOWN');
select * from dim_indicator_type;

CREATE TABLE indicator_fact_1 (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol_id INT NOT NULL,
    interval_id INT NOT NULL,
    type_id INT NOT NULL,       
    value DECIMAL(18,8),    -- giá trị chỉ số
    timestamp DATETIME,     -- từ close_time của kline_fact
    FOREIGN KEY (symbol_id) REFERENCES symbol_dim_1(symbol_id),
    FOREIGN KEY (interval_id) REFERENCES interval_dim_1(interval_id),
    FOREIGN KEY (type_id) REFERENCES dim_indicator_type_1(type_id),
    UNIQUE(symbol_id, interval_id, type_id, timestamp)  -- đảm bảo atomic, tránh trùng lặp
);