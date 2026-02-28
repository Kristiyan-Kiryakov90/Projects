CREATE OR REPLACE VIEW v_Product_Enriched AS
SELECT
    p.product_id        AS product_id,
    p.product_name      AS product_name,
    c.category_name     AS category_name,
    s.company_name      AS supplier,
    p.unit_price        AS unit_price
FROM products p
LEFT JOIN categories c
    ON c.category_id = p.category_id
LEFT JOIN suppliers s
    ON s.supplier_id = p.supplier_id;


CREATE OR REPLACE VIEW v_Order_Sales AS
SELECT
    o.order_id,
    o.customer_id,
    o.employee_id,
    o.order_date,
    o.required_date,
    o.shipped_date,
    o.ship_via,
    o.ship_country,
    od.product_id,
    od.unit_price,
    od.quantity,
    od.discount,
    (od.unit_price * od.quantity * (1 - od.discount)) AS totallineamount
FROM orders o
JOIN order_details od
    ON od.order_id = o.order_id;


CREATE OR REPLACE VIEW v_Customer_Geo AS
SELECT DISTINCT
    c.customer_id,
    c.company_name,
    c.city,
    c.country
FROM customers c
JOIN orders o
    ON o.customer_id = c.customer_id;

