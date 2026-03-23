CREATE DATABASE IF NOT EXISTS requestsdb;
USE requestsdb;

CREATE TABLE IF NOT EXISTS requests (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    country VARCHAR(100),
    client_ip VARCHAR(45),
    gender VARCHAR(20),
    age INT,
    income DOUBLE,
    is_banned BOOLEAN,
    time_of_day VARCHAR(50),
    requested_file VARCHAR(255),
    http_method VARCHAR(16),
    response_code INT
);

CREATE TABLE IF NOT EXISTS failed_requests (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    requested_file VARCHAR(255),
    error_code INT
);
