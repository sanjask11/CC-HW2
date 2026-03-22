-- Create database for successful requests
CREATE TABLE IF NOT EXISTS successful_requests (
    id INT AUTO_INCREMENT PRIMARY KEY,
    country VARCHAR(100),
    client_ip VARCHAR(45),
    gender VARCHAR(10),
    age INT,
    income VARCHAR(50),
    is_banned BOOLEAN,
    time_of_day DATETIME,
    requested_file VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_country (country),
    INDEX idx_time (time_of_day)
);

-- Create database for failed requests
CREATE TABLE IF NOT EXISTS failed_requests (
    id INT AUTO_INCREMENT PRIMARY KEY,
    time_of_request DATETIME,
    requested_file VARCHAR(255),
    error_code INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_error (error_code),
    INDEX idx_time (time_of_request)
);
