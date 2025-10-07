use thesis;
CREATE TABLE dim_tag_1 (
    tag_id INT AUTO_INCREMENT PRIMARY KEY,
    tag_name VARCHAR(255) UNIQUE
);

CREATE TABLE news_fact_1 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    url VARCHAR(500) NOT NULL UNIQUE,
    sentiment_score FLOAT NOT NULL,
    created_date DATETIME NOT NULL,
    view_number INT NULL,
    tag_id INT,
    FOREIGN KEY (tag_id) REFERENCES dim_tag_1(tag_id)
);