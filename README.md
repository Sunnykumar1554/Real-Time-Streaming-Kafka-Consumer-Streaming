# Real-Time-Streaming-Kafka-Consumer-Streaming
Goal :  
  1. A producer sends order data to Kafka
  2. A consumer reads the data
  3. The same data is stored in PostgreSQL
  4. Students verify the stored data using pgAdmin
This simulates how real e-commerce systems store live transactions.

For the PostgreSQL :
  Create database: shop_stream_db 
  Create table: CREATE TABLE orders_log ( 
    id SERIAL PRIMARY KEY, 
    order_id INT, 
    customer TEXT, 
    product TEXT, 
    quantity INT, 
    amount NUMERIC, 
    received_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
    ); 
