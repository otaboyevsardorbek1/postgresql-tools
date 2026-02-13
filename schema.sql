-- =====================================================
-- Enterprise PostgreSQL Schema Template
-- Includes security best practices, indexing strategy,
-- partitioning, and audit logging
-- =====================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- =====================================================
-- Audit Logging System
-- =====================================================
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    operation CHAR(1) NOT NULL, -- I: Insert, U: Update, D: Delete
    old_data JSONB,
    new_data JSONB,
    changed_by TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    client_addr INET,
    application_name TEXT
);

-- Audit trigger function
CREATE OR REPLACE FUNCTION audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit_log (
        table_name, operation, old_data, new_data, 
        changed_by, client_addr, application_name
    ) VALUES (
        TG_TABLE_NAME,
        TG_OP,
        CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN row_to_json(OLD)::JSONB ELSE NULL END,
        CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN row_to_json(NEW)::JSONB ELSE NULL END,
        current_user,
        inet_client_addr(),
        current_setting('application_name', TRUE)
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- =====================================================
-- Users & Profiles (Partitioned)
-- =====================================================
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    full_name VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    is_verified BOOLEAN DEFAULT FALSE,
    role VARCHAR(50) DEFAULT 'user',
    metadata JSONB DEFAULT '{}',
    last_login TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ -- Soft delete
) PARTITION BY RANGE (created_at);

-- Create partitions
CREATE TABLE users_2024 PARTITION OF users
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE users_2025 PARTITION OF users
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE users_default PARTITION OF users DEFAULT;

-- =====================================================
-- Products Table with Full-Text Search
-- =====================================================
CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    price DECIMAL(12,2) NOT NULL,
    cost DECIMAL(12,2),
    quantity INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    attributes JSONB,
    search_vector TSVECTOR,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Full-text search index
CREATE INDEX idx_products_search ON products USING GIN(search_vector);

-- Trigger for search vector update
CREATE OR REPLACE FUNCTION products_search_update()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector = 
        setweight(to_tsvector('english', COALESCE(NEW.name, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.sku, '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(NEW.description, '')), 'C') ||
        setweight(to_tsvector('english', COALESCE(NEW.category, '')), 'D');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_products_search
    BEFORE INSERT OR UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION products_search_update();

-- =====================================================
-- Orders with Referential Integrity
-- =====================================================
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    subtotal DECIMAL(12,2) NOT NULL,
    tax DECIMAL(12,2) NOT NULL,
    shipping DECIMAL(12,2) NOT NULL,
    total DECIMAL(12,2) NOT NULL,
    shipping_address JSONB NOT NULL,
    billing_address JSONB NOT NULL,
    payment_method VARCHAR(50),
    payment_status VARCHAR(50) DEFAULT 'pending',
    notes TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Order items
CREATE TABLE IF NOT EXISTS order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(12,2) NOT NULL,
    total_price DECIMAL(12,2) NOT NULL,
    discount DECIMAL(12,2) DEFAULT 0,
    metadata JSONB
);

-- =====================================================
-- Indexes for Performance
-- =====================================================
-- Users indexes
CREATE INDEX idx_users_username ON users(username) WHERE deleted_at IS NULL;
CREATE INDEX idx_users_email ON users(email) WHERE deleted_at IS NULL;
CREATE INDEX idx_users_created_at ON users(created_at);
CREATE INDEX idx_users_role ON users(role) WHERE is_active = TRUE;

-- Products indexes
CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_attributes ON products USING GIN(attributes);
CREATE INDEX idx_products_created_at ON products(created_at);

-- Orders indexes
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_orders_order_number ON orders(order_number);
CREATE INDEX idx_orders_payment_status ON orders(payment_status);

-- =====================================================
-- Views for Common Queries
-- =====================================================
-- User order summary
CREATE OR REPLACE VIEW user_order_summary AS
SELECT 
    u.id AS user_id,
    u.username,
    u.email,
    COUNT(o.id) AS total_orders,
    SUM(o.total) AS total_spent,
    AVG(o.total) AS average_order_value,
    MAX(o.created_at) AS last_order_date
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.deleted_at IS NULL
GROUP BY u.id, u.username, u.email;

-- Product performance
CREATE OR REPLACE VIEW product_performance AS
SELECT 
    p.id,
    p.sku,
    p.name,
    p.category,
    COALESCE(SUM(oi.quantity), 0) AS units_sold,
    COALESCE(COUNT(DISTINCT oi.order_id), 0) AS order_count,
    COALESCE(SUM(oi.total_price), 0) AS revenue,
    COALESCE(AVG(oi.unit_price), 0) AS avg_selling_price
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.id AND o.status NOT IN ('cancelled', 'refunded')
GROUP BY p.id, p.sku, p.name, p.category;

-- =====================================================
-- Functions & Procedures
-- =====================================================
-- Update product quantity
CREATE OR REPLACE FUNCTION update_product_quantity(
    p_product_id UUID,
    p_quantity_change INTEGER
)
RETURNS INTEGER AS $$
DECLARE
    new_quantity INTEGER;
BEGIN
    UPDATE products 
    SET quantity = quantity + p_quantity_change,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_product_id
    RETURNING quantity INTO new_quantity;
    
    RETURN new_quantity;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Monthly order summary function
CREATE OR REPLACE FUNCTION get_monthly_order_summary(
    p_year INTEGER,
    p_month INTEGER
)
RETURNS TABLE (
    order_date DATE,
    total_orders BIGINT,
    total_revenue DECIMAL,
    avg_order_value DECIMAL,
    unique_customers BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        DATE(o.created_at) as order_date,
        COUNT(*) as total_orders,
        SUM(o.total) as total_revenue,
        AVG(o.total) as avg_order_value,
        COUNT(DISTINCT o.user_id) as unique_customers
    FROM orders o
    WHERE EXTRACT(YEAR FROM o.created_at) = p_year
        AND EXTRACT(MONTH FROM o.created_at) = p_month
        AND o.status NOT IN ('cancelled', 'refunded')
    GROUP BY DATE(o.created_at)
    ORDER BY order_date;
END;
$$ LANGUAGE plpgsql STABLE;

-- =====================================================
-- Row Level Security Policies
-- =====================================================
-- Enable RLS
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;

-- Users can only see their own data
CREATE POLICY user_access ON users
    USING (id = current_user_id() OR current_user = 'postgres');

-- Orders policies
CREATE POLICY order_select ON orders FOR SELECT
    USING (user_id = current_user_id() OR current_user = 'postgres');

CREATE POLICY order_insert ON orders FOR INSERT
    WITH CHECK (user_id = current_user_id());

-- =====================================================
-- Maintenance Functions
-- =====================================================
-- Update statistics
CREATE OR REPLACE FUNCTION maintenance_update_stats()
RETURNS void AS $$
BEGIN
    ANALYZE users;
    ANALYZE products;
    ANALYZE orders;
    ANALYZE order_items;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Initial Data
-- =====================================================
-- Insert admin user (password: '32905002Os!') usernmae:admin
INSERT INTO users (id, username, email, password_hash, full_name, role, is_verified)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'admin',
    'otaboyevsardorbek295@gmail.com',
    crypt('32905002Os!', gen_salt('bf', 12)),
    'System Administrator',
    'admin',
    TRUE
) ON CONFLICT DO NOTHING;
