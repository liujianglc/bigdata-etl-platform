-- Create sample orders table
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    order_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO orders (product_id, customer_id, amount, order_date, status) VALUES
('PROD001', 'CUST001', 99.99, '2024-01-15', 'completed'),
('PROD002', 'CUST002', 149.50, '2024-01-15', 'completed'),
('PROD001', 'CUST003', 99.99, '2024-01-16', 'pending'),
('PROD003', 'CUST001', 299.00, '2024-01-16', 'completed'),
('PROD002', 'CUST004', 149.50, '2024-01-17', 'completed'),
('PROD001', 'CUST005', 99.99, '2024-01-17', 'cancelled'),
('PROD004', 'CUST002', 199.99, '2024-01-18', 'completed'),
('PROD003', 'CUST006', 299.00, '2024-01-18', 'pending'),
('PROD002', 'CUST007', 149.50, '2024-01-19', 'completed'),
('PROD001', 'CUST008', 99.99, '2024-01-19', 'completed');