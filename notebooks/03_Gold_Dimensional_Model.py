#!/usr/bin/env python
# coding: utf-8

# ## 03_Gold_Dimensional_Model
# 
# null

# In[ ]:


# Welcome to your new notebook
# Type here in the cell editor to add code!


# In[1]:


df_customers = spark.table("silver_customers")
df_products = spark.table("silver_products")
df_stores = spark.table("silver_stores")
df_sales = spark.table("silver_sales_final")
df_returns = spark.table("silver_returns_final")
df_inventory = spark.table("silver_inventory_final")

print("Silver tables loaded")


# In[2]:


from pyspark.sql.functions import col

dim_customer = (
    df_customers
    .withColumn("CustomerID", col("CustomerID").cast("long"))
    .select(
        "CustomerID",
        "FirstName",
        "LastName",
        "Email",
        "Gender",
        "DateOfBirth",
        "Country",
        "City",
        "RegistrationDate",
        "CustomerSegment"
    )
    .dropDuplicates(["CustomerID"])
)

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_customer")

print(f"dim_customer: {dim_customer.count():,} rows")


# In[3]:


dim_product = (
    df_products
    .select(
        "ProductID",
        "ProductName",
        "Category",
        "SubCategory",
        "Brand",
        "SupplierID",
        "UnitCost",
        "UnitPrice"
    )
    .dropDuplicates(["ProductID"])
)

dim_product.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_product")

print(f"dim_product: {dim_product.count():,} rows")


# In[4]:


dim_store = (
    df_stores
    .select(
        "StoreID",
        "StoreName",
        "Country",
        "City",
        "Region",
        "StoreType",
        "OpeningDate"
    )
    .dropDuplicates(["StoreID"])
)

dim_store.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_store")

print(f"dim_store: {dim_store.count():,} rows")


# In[5]:


from pyspark.sql.functions import (
    col,
    min,
    max,
    explode,
    sequence,
    to_date,
    year,
    month,
    dayofmonth,
    dayofweek,
    date_format,
    quarter,
    weekofyear
)

sales_dates = df_sales.select(
    col("OrderDate").alias("Date")
)

return_dates = df_returns.select(
    col("ReturnDate").alias("Date")
)

inventory_dates = df_inventory.select(
    col("Date")
)

all_dates = (
    sales_dates
    .union(return_dates)
    .union(inventory_dates)
)

date_range = all_dates.select(
    min("Date").alias("MinDate"),
    max("Date").alias("MaxDate")
).collect()[0]

min_date = date_range["MinDate"]
max_date = date_range["MaxDate"]

print("Minimum date:", min_date)
print("Maximum date:", max_date)


# In[6]:


dim_date = (
    spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{min_date}'),
                to_date('{max_date}'),
                interval 1 day
            )
        ) AS Date
        """
    )
    .withColumn(
        "DateKey",
        date_format(col("Date"), "yyyyMMdd").cast("int")
    )
    .withColumn("Year", year(col("Date")))
    .withColumn("Quarter", quarter(col("Date")))
    .withColumn("MonthNumber", month(col("Date")))
    .withColumn("MonthName", date_format(col("Date"), "MMMM"))
    .withColumn("Day", dayofmonth(col("Date")))
    .withColumn("DayName", date_format(col("Date"), "EEEE"))
    .withColumn("DayOfWeek", dayofweek(col("Date")))
    .withColumn("WeekOfYear", weekofyear(col("Date")))
)

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_date")

print(f"dim_date: {dim_date.count():,} rows")


# In[7]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT 'dim_customer' AS TableName, COUNT(*) AS RowCount
# FROM dim_customer

# UNION ALL

# SELECT 'dim_product', COUNT(*)
# FROM dim_product

# UNION ALL

# SELECT 'dim_store', COUNT(*)
# FROM dim_store

# UNION ALL

# SELECT 'dim_date', COUNT(*)
# FROM dim_date;


# In[8]:


from pyspark.sql.functions import col, date_format

fact_sales = (
    df_sales
    .withColumn("CustomerID", col("CustomerID").cast("long"))
    .withColumn(
        "DateKey",
        date_format(col("OrderDate"), "yyyyMMdd").cast("int")
    )
    .select(
        "SalesID",
        "OrderID",
        "DateKey",
        "CustomerID",
        "ProductID",
        "StoreID",
        "Quantity",
        "UnitPrice",
        "Discount",
        "GrossAmount",
        "DiscountAmount",
        "NetSalesAmount",
        "PaymentMethod",
        "SalesChannel"
    )
)

fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_sales")

print(f"fact_sales: {fact_sales.count():,} rows")


# In[9]:


fact_returns = (
    df_returns
    .withColumn("CustomerID", col("CustomerID").cast("long"))
    .withColumn(
        "DateKey",
        date_format(col("ReturnDate"), "yyyyMMdd").cast("int")
    )
    .select(
        "ReturnID",
        "OrderID",
        "DateKey",
        "CustomerID",
        "ProductID",
        "ReturnQuantity",
        "ReturnReason"
    )
)

fact_returns.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_returns")

print(f"fact_returns: {fact_returns.count():,} rows")


# In[2]:


fact_inventory = (
    df_inventory
    .withColumn(
        "DateKey",
        date_format(col("Date"), "yyyyMMdd").cast("int")
    )
    .select(
        "InventoryID",
        "DateKey",
        "ProductID",
        "StoreID",
        "StockQuantity",
        "ReorderLevel"
    )
)

fact_inventory.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_inventory")

print(f"fact_inventory: {fact_inventory.count():,} rows")


# In[12]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT
#     ROUND(SUM(NetSalesAmount), 2) AS TotalRevenue,
#     COUNT(DISTINCT OrderID) AS TotalOrders,
#     SUM(Quantity) AS UnitsSold
# FROM fact_sales;


# In[13]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT
#     s.Country,
#     ROUND(SUM(f.NetSalesAmount), 2) AS Revenue
# FROM fact_sales f
# JOIN dim_store s
#     ON f.StoreID = s.StoreID
# GROUP BY s.Country
# ORDER BY Revenue DESC;


# In[14]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT
#     p.Category,
#     ROUND(SUM(f.NetSalesAmount), 2) AS Revenue,
#     SUM(f.Quantity) AS UnitsSold
# FROM fact_sales f
# JOIN dim_product p
#     ON f.ProductID = p.ProductID
# GROUP BY p.Category
# ORDER BY Revenue DESC;


# In[15]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT
#     COUNT(*) AS LowStockRecords
# FROM fact_inventory
# WHERE StockQuantity <= ReorderLevel;


# In[ ]:


gold_tables = [
    "dim_customer",
    "dim_product",
    "dim_store",
    "dim_date",
    "fact_sales",
    "fact_returns",
    "fact_inventory"
]

for table_name in gold_tables:
    df = spark.table(table_name)
    print(f"\n--- {table_name} ---")
    print(f"Rows: {df.count():,}")
    df.printSchema()

