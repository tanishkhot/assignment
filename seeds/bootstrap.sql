-- Basic sample data for local Postgres
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.customers (
  id BIGSERIAL PRIMARY KEY,
  email TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS public.orders (
  id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT REFERENCES public.customers(id),
  amount NUMERIC(10,2),
  created_at TIMESTAMP DEFAULT now()
);

-- Seed customers
INSERT INTO public.customers (email) VALUES ('alice@example.com') ON CONFLICT DO NOTHING;
INSERT INTO public.customers (email) VALUES ('bob@example.com')   ON CONFLICT DO NOTHING;

-- Seed orders if not present
INSERT INTO public.orders (customer_id, amount)
SELECT c.id, 19.99 FROM public.customers c
WHERE c.email = 'alice@example.com' AND NOT EXISTS (
  SELECT 1 FROM public.orders o WHERE o.customer_id = c.id AND o.amount = 19.99
);

INSERT INTO public.orders (customer_id, amount)
SELECT c.id, 42.50 FROM public.customers c
WHERE c.email = 'bob@example.com' AND NOT EXISTS (
  SELECT 1 FROM public.orders o WHERE o.customer_id = c.id AND o.amount = 42.50
);

