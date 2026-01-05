Data Dictionary for Gold Layer 

Overview 
The gold layer is the business-level data representation, a structure to support analysis and reporting use cases. It contains a dimension and fact table for specific business metrics.



1.	gold.dim.customer

. Purpose: It stores customer details, including demography and geography
. Columns:


Column Name 	         Data Type             	Description

customer_key	          INT                	It’s a segregated key that uniquely identifies each customer in the dimension table
customer_id	            INT	                Unique Numerical identifier assigned to each customer
customer_no             NVARCHAR (50)	      An alphabetical identifier representing the customer and used to reference and track.
first_name	            NVARCHAR(50)	      Customer fist name
last name	              NVARCHARE(50)	      Customer's last name
marital_status	        NVARCHAR (50)	      Customer Marital Status, eg. (‘Married’, ‘Single’)
created_date	          DATE              	Date and time the customer record was created in the system
birth_date	            DATE	              Customer Date of Birth formatted as YYY-MM-DD (1989-12-04)
gendr	                  NVARCHAR (50)	      Customer Gender eg. (‘Female’, ‘Male’, ‘N/A’)
Country	                NVARCHAR (50)	      Customer country of residence eg. (‘Australia’, ‘Germany’)







2.	gold.dim.products

. Purpose: Stores all the information about product categories, cost, products line etc..
. Columns

Column Name 	         Data Type	        Description

product_key	           INT	              It’s a segregated key that uniquely identifies each product in the dimension table
product_id	           INT	              An alphabetical identifier representing the products and used for internal reference and tracking.
category_id	           NVARCHAR(50)	      A unique ID for a product category that links to the highest level of classification
product_number         NVARCHAR(50)     	Structured Alphabetical code representing the products used for categorization or inventory
product_name	         NVARCHAR(50)	      Product name that describes the product, including size, color, and type
Cost	                 INT	              Product cost or base price for a product is measured in units 
product_line	         NVARCHAR(50)	      Product line or series to which the product belongs 
start_date	           DATE	              Date of the product start
subcategory	           NVARCHAR(50)	      A product subcategory is a more detailed classification of the product category
category	             NVARCHAR(50)	      Product category is a broad classification of products, eg. (Bike, Components, Accessories)  







3.	gold.fact.products
. Purpose: Stores all the information about product categories, cost, product lines etc…
. Columns:

Column Name 	   Data Type	        Description

order_number	   NVARCHAR(50)	      Unique Alphabetical identifier for each seal order eg. (S043697)
product_key	     INT	              The segregate key used  to link the orders in the fact table with the dimension tables  
customer_key	   INT	              The segregate key used  to link the orders in the fact table with the dimension tables  
shipment_date	   DATE	              Order shipment date when the product is delivered to the customer 
due_date	       DATE	              The date that the order payment  expired 
seals	           INT	              The total sales value for line items in whole currency e.g ( ‘25’ )
quantity	       INT	              The number of units of products ordered for line item e.g (1,2 ,3)
price	           INT	              The unit price of the product for line items e.g (35, 29)
create_date	     DATE              	Date and time the customer record was created in the system











