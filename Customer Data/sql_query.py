# This is our sql_query.py

def generate_sql_query():
 query = """
  SELECT
   orders.order_id, orders.order_date,
   customers.customer_name, customers.customer_email
  FROM 
   orders
  JOIN 
   customers ON orders.customer_id = customers.customer_id;
 """
 return query