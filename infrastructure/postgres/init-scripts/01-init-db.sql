-- =============================================
-- СКРИПТ ИНИЦИАЛИЗАЦИИ БАЗЫ ДАННЫХ
-- Выполняется автоматически при первом запуске
-- =============================================

-- Создать расширение pg_stat_statements
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Создать схему для приложения
CREATE SCHEMA IF NOT EXISTS myapp;

-- Создать пользователя для приложения (с ограниченными правами)
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'app_user') THEN
      CREATE USER app_user WITH PASSWORD 'AppPassword123';
   END IF;
END
$$;

-- Выдать права на схему
GRANT USAGE ON SCHEMA myapp TO app_user;
GRANT CREATE ON SCHEMA myapp TO app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA myapp GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_user;

-- =============================================
-- СОЗДАНИЕ ТЕСТОВЫХ ТАБЛИЦ
-- =============================================

-- Таблица пользователей
CREATE TABLE IF NOT EXISTS myapp.users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Таблица заказов
CREATE TABLE IF NOT EXISTS myapp.orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES myapp.users(id),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Таблица товаров в заказе
CREATE TABLE IF NOT EXISTS myapp.order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES myapp.orders(id),
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- =============================================
-- СОЗДАНИЕ ИНДЕКСОВ
-- =============================================

-- Индекс на email (для быстрого поиска пользователей)
CREATE INDEX IF NOT EXISTS idx_users_email ON myapp.users(email);

-- Индекс на user_id в заказах (для JOIN)
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON myapp.orders(user_id);

-- Индекс на статус заказа (часто фильтруем по статусу)
CREATE INDEX IF NOT EXISTS idx_orders_status ON myapp.orders(status);

-- Индекс на order_id в товарах (для JOIN)
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON myapp.order_items(order_id);

-- =============================================
-- ВСТАВКА ТЕСТОВЫХ ДАННЫХ
-- =============================================

-- Вставить тестовых пользователей
INSERT INTO myapp.users (email, username) VALUES
    ('user1@example.com', 'user1'),
    ('user2@example.com', 'user2'),
    ('user3@example.com', 'user3'),
    ('admin@example.com', 'admin')
ON CONFLICT (email) DO NOTHING;

-- Вставить тестовые заказы
INSERT INTO myapp.orders (user_id, order_number, status, total_amount) VALUES
    (1, 'ORD-001', 'completed', 150.00),
    (1, 'ORD-002', 'pending', 250.00),
    (2, 'ORD-003', 'completed', 99.99),
    (3, 'ORD-004', 'cancelled', 300.00)
ON CONFLICT (order_number) DO NOTHING;

-- Вставить товары в заказы
INSERT INTO myapp.order_items (order_id, product_name, quantity, price) VALUES
    (1, 'Товар A', 2, 50.00),
    (1, 'Товар B', 1, 50.00),
    (2, 'Товар C', 5, 50.00),
    (3, 'Товар A', 1, 99.99),
    (4, 'Товар D', 3, 100.00);

-- =============================================
-- ВЫВОД ИНФОРМАЦИИ
-- =============================================

-- Показать количество созданных записей
DO $$
BEGIN
    RAISE NOTICE 'Инициализация завершена успешно!';
    RAISE NOTICE 'Создано пользователей: %', (SELECT COUNT(*) FROM myapp.users);
    RAISE NOTICE 'Создано заказов: %', (SELECT COUNT(*) FROM myapp.orders);
    RAISE NOTICE 'Создано товаров: %', (SELECT COUNT(*) FROM myapp.order_items);
END $$;
