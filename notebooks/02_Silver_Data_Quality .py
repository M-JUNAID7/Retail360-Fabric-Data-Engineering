#!/usr/bin/env python
# coding: utf-8

# ## 02_Silver_Data_Quality
# 
# null

# In[ ]:


# Welcome to your new notebook
# Type here in the cell editor to add code!


# In[ ]:


df_sales = spark.table("bronze_sales")

print(f"Total rows: {df_sales.count():,}")

display(df_sales.limit(10))


# In[1]:


print("Spark session is working")


# In[2]:


df_sales = spark.table("bronze_sales")

print("Columns:", df_sales.columns)
df_sales.printSchema()


# In[3]:


total_rows = df_sales.count()
print(f"Total rows: {total_rows:,}")


# In[4]:


from pyspark.sql.functions import col, sum

null_report = df_sales.select(
    *[
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df_sales.columns
    ]
)

display(null_report)


# In[5]:


negative_quantity_count = (
    df_sales
    .filter(col("Quantity") < 0)
    .count()
)

print(f"Negative quantity records: {negative_quantity_count:,}")


# In[6]:


from pyspark.sql.functions import count

duplicate_sales = (
    df_sales
    .groupBy("SalesID")
    .agg(count("*").alias("RecordCount"))
    .filter(col("RecordCount") > 1)
)

duplicate_sales_id_count = duplicate_sales.count()

print(f"Duplicate SalesIDs: {duplicate_sales_id_count:,}")

display(duplicate_sales.limit(10))


# In[7]:


invalid_discount_count = (
    df_sales
    .filter(
        (col("Discount") < 0) |
        (col("Discount") > 1)
    )
    .count()
)

print(f"Invalid discount records: {invalid_discount_count:,}")


# In[8]:


df_products = spark.table("bronze_products")


# In[9]:


invalid_product_prices = (
    df_products
    .filter(col("UnitPrice") <= 0)
)

print(
    f"Invalid product prices: "
    f"{invalid_product_prices.count():,}"
)

display(invalid_product_prices)


# In[10]:


missing_product_costs = (
    df_products
    .filter(col("UnitCost").isNull())
)

print(
    f"Missing product costs: "
    f"{missing_product_costs.count():,}"
)

display(missing_product_costs)


# In[11]:


from pyspark.sql.functions import (
    col,
    lit,
    when,
    current_timestamp
)


# In[12]:


df_sales_dedup = df_sales.dropDuplicates(["SalesID"])


# In[13]:


print(f"Before deduplication: {df_sales.count():,}")
print(f"After deduplication:  {df_sales_dedup.count():,}")


# In[14]:


df_sales_cleaned = (
    df_sales_dedup
    .withColumn(
        "Discount",
        when(
            col("Discount").isNull(),
            lit(0.0)
        ).otherwise(col("Discount"))
    )
)


# In[15]:


null_discount_count = (
    df_sales_cleaned
    .filter(col("Discount").isNull())
    .count()
)

print(f"NULL discounts remaining: {null_discount_count}")


# In[16]:


valid_condition = (
    (col("CustomerID").isNotNull()) &
    (col("Quantity") > 0) &
    (col("Discount") >= 0) &
    (col("Discount") <= 1) &
    (col("UnitPrice") > 0)
)


# In[18]:


df_sales_quarantine = (
    df_sales_cleaned
    .filter(~valid_condition)
    .withColumn(
        "RejectionReason",
        when(
            col("CustomerID").isNull(),
            lit("NULL_CUSTOMER_ID")
        )
        .when(
            col("Quantity") <= 0,
            lit("INVALID_QUANTITY")
        )
        .when(
            (col("Discount") < 0) | (col("Discount") > 1),
            lit("INVALID_DISCOUNT")
        )
        .when(
            col("UnitPrice") <= 0,
            lit("INVALID_UNIT_PRICE")
        )
        .otherwise(
            lit("UNKNOWN_QUALITY_ERROR")
        )
    )
    .withColumn(
        "QuarantinedAt",
        current_timestamp()
    )
)


# In[19]:


df_sales_valid = (
    df_sales_cleaned
    .filter(valid_condition)
)


# In[20]:


df_sales_valid = (
    df_sales_valid
    .withColumn(
        "GrossAmount",
        col("Quantity") * col("UnitPrice")
    )
    .withColumn(
        "DiscountAmount",
        (col("Quantity") * col("UnitPrice")) * col("Discount")
    )
    .withColumn(
        "NetSalesAmount",
        (col("Quantity") * col("UnitPrice")) *
        (lit(1) - col("Discount"))
    )
)


# In[21]:


df_sales_valid = (
    df_sales_valid
    .withColumn(
        "SilverProcessedAt",
        current_timestamp()
    )
)


# In[22]:


display(
    df_sales_valid.select(
        "SalesID",
        "OrderID",
        "Quantity",
        "UnitPrice",
        "Discount",
        "GrossAmount",
        "DiscountAmount",
        "NetSalesAmount",
        "SilverProcessedAt"
    ).limit(20)
)


# In[23]:


display(
    df_sales_quarantine
    .groupBy("RejectionReason")
    .count()
)


# In[24]:


dedup_count = df_sales_dedup.count()
valid_count = df_sales_valid.count()
quarantine_count = df_sales_quarantine.count()

print(f"After deduplication : {dedup_count:,}")
print(f"Valid records       : {valid_count:,}")
print(f"Quarantined records : {quarantine_count:,}")
print(f"Reconciled total    : {valid_count + quarantine_count:,}")


# In[25]:


(
    df_sales_valid.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_sales")
)


# In[26]:


(
    df_sales_quarantine.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("quarantine_sales")
)


# In[27]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT
#     'Bronze' AS Layer,
#     COUNT(*) AS RowCount
# FROM bronze_sales

# UNION ALL

# SELECT
#     'Silver',
#     COUNT(*)
# FROM silver_sales

# UNION ALL

# SELECT
#     'Quarantine',
#     COUNT(*)
# FROM quarantine_sales;


# In[29]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT
#     RejectionReason,
#     COUNT(*) AS RejectedRecords
# FROM quarantine_sales
# GROUP BY RejectionReason
# ORDER BY RejectedRecords DESC;


# In[30]:


df_customers = spark.table("bronze_customers")

print(f"Bronze customers: {df_customers.count():,}")
df_customers.printSchema()


# In[31]:


from pyspark.sql.functions import col, trim, initcap, lower, current_timestamp

display(
    df_customers.select(
        *[
            sum(col(c).isNull().cast("int")).alias(c)
            for c in df_customers.columns
        ]
    )
)


# In[32]:


df_customers_clean = (
    df_customers
    .dropDuplicates(["CustomerID"])

    .withColumn(
        "FirstName",
        initcap(trim(col("FirstName")))
    )

    .withColumn(
        "LastName",
        initcap(trim(col("LastName")))
    )

    .withColumn(
        "Email",
        lower(trim(col("Email")))
    )

    .withColumn(
        "Country",
        initcap(trim(col("Country")))
    )

    .withColumn(
        "City",
        initcap(trim(col("City")))
    )

    .withColumn(
        "CustomerSegment",
        initcap(trim(col("CustomerSegment")))
    )

    .withColumn(
        "SilverProcessedAt",
        current_timestamp()
    )
)


# In[33]:


invalid_customers = (
    df_customers_clean
    .filter(col("CustomerID").isNull())
)

print(
    f"Invalid CustomerID records: "
    f"{invalid_customers.count():,}"
)


# In[34]:


print(
    f"Silver customers: "
    f"{df_customers_clean.count():,}"
)


# In[35]:


(
    df_customers_clean.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_customers")
)


# In[36]:


df_products = spark.table("bronze_products")

print(f"Bronze products: {df_products.count():,}")


# In[37]:


product_valid_condition = (
    col("ProductID").isNotNull()
    & (col("UnitPrice") > 0)
    & col("UnitCost").isNotNull()
    & (col("UnitCost") >= 0)
)


# In[38]:


df_products_valid = (
    df_products
    .dropDuplicates(["ProductID"])
    .filter(product_valid_condition)

    .withColumn("ProductName", trim(col("ProductName")))
    .withColumn("Category", initcap(trim(col("Category"))))
    .withColumn("SubCategory", initcap(trim(col("SubCategory"))))
    .withColumn("Brand", trim(col("Brand")))

    .withColumn(
        "SilverProcessedAt",
        current_timestamp()
    )
)


# In[39]:


from pyspark.sql.functions import when, lit

df_products_quarantine = (
    df_products
    .dropDuplicates(["ProductID"])
    .filter(~product_valid_condition)

    .withColumn(
        "RejectionReason",

        when(
            col("ProductID").isNull(),
            lit("NULL_PRODUCT_ID")
        )

        .when(
            col("UnitPrice") <= 0,
            lit("INVALID_UNIT_PRICE")
        )

        .when(
            col("UnitCost").isNull(),
            lit("NULL_UNIT_COST")
        )

        .when(
            col("UnitCost") < 0,
            lit("INVALID_UNIT_COST")
        )

        .otherwise(
            lit("UNKNOWN_QUALITY_ERROR")
        )
    )

    .withColumn(
        "QuarantinedAt",
        current_timestamp()
    )
)


# In[40]:


product_valid_count = df_products_valid.count()
product_quarantine_count = df_products_quarantine.count()

print(f"Bronze products      : {df_products.count():,}")
print(f"Valid products       : {product_valid_count:,}")
print(f"Quarantined products : {product_quarantine_count:,}")
print(
    f"Reconciled           : "
    f"{product_valid_count + product_quarantine_count:,}"
)


# In[41]:


display(
    df_products_quarantine
    .groupBy("RejectionReason")
    .count()
)


# In[42]:


(
    df_products_valid.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_products")
)

(
    df_products_quarantine.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("quarantine_products")
)


# In[44]:


display(df_customers.limit(10))


# In[45]:


df_customers.printSchema()


# In[49]:


df_stores = spark.table("bronze_stores")

df_stores_clean = (
    df_stores
    .dropDuplicates(["StoreID"])
    .withColumn("StoreName", trim(col("StoreName")))
    .withColumn("Country", initcap(trim(col("Country"))))
    .withColumn("City", initcap(trim(col("City"))))
    .withColumn("Region", trim(col("Region")))
    .withColumn("StoreType", initcap(trim(col("StoreType"))))
    .withColumn("SilverProcessedAt", current_timestamp())
)

df_stores_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_stores")

print(f"silver_stores created: {df_stores_clean.count():,} rows")


# In[50]:


df_returns = spark.table("bronze_returns")

df_returns_clean = (
    df_returns
    .dropDuplicates(["ReturnID"])
    .filter(col("ReturnID").isNotNull())
    .filter(col("OrderID").isNotNull())
    .filter(col("ProductID").isNotNull())
    .filter(col("ReturnQuantity") > 0)
    .withColumn("ReturnReason", initcap(trim(col("ReturnReason"))))
    .withColumn("SilverProcessedAt", current_timestamp())
)

df_returns_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_returns")

print(f"Bronze returns: {df_returns.count():,}")
print(f"Silver returns: {df_returns_clean.count():,}")


# In[51]:


df_inventory = spark.table("bronze_inventory")

df_inventory_clean = (
    df_inventory
    .dropDuplicates(["InventoryID"])
    .filter(col("InventoryID").isNotNull())
    .filter(col("StoreID").isNotNull())
    .filter(col("ProductID").isNotNull())
    .filter(col("StockQuantity") >= 0)
    .filter(col("ReorderLevel") >= 0)
    .withColumn("SilverProcessedAt", current_timestamp())
)

df_inventory_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_inventory")

print(f"Bronze inventory: {df_inventory.count():,}")
print(f"Silver inventory: {df_inventory_clean.count():,}")


# In[52]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT 'Customers' AS Dataset, COUNT(*) AS RowCount
# FROM silver_customers

# UNION ALL

# SELECT 'Products', COUNT(*)
# FROM silver_products

# UNION ALL

# SELECT 'Stores', COUNT(*)
# FROM silver_stores

# UNION ALL

# SELECT 'Sales', COUNT(*)
# FROM silver_sales

# UNION ALL

# SELECT 'Returns', COUNT(*)
# FROM silver_returns

# UNION ALL

# SELECT 'Inventory', COUNT(*)
# FROM silver_inventory;


# In[53]:


invalid_customer_refs = (
    spark.table("silver_sales")
    .join(
        spark.table("silver_customers").select("CustomerID"),
        on="CustomerID",
        how="left_anti"
    )
)

print(
    f"Sales with invalid CustomerID: "
    f"{invalid_customer_refs.count():,}"
)


# In[54]:


invalid_product_refs = (
    spark.table("silver_sales")
    .join(
        spark.table("silver_products").select("ProductID"),
        on="ProductID",
        how="left_anti"
    )
)

print(
    f"Sales with invalid ProductID: "
    f"{invalid_product_refs.count():,}"
)


# In[55]:


invalid_store_refs = (
    spark.table("silver_sales")
    .join(
        spark.table("silver_stores").select("StoreID"),
        on="StoreID",
        how="left_anti"
    )
)

print(
    f"Sales with invalid StoreID: "
    f"{invalid_store_refs.count():,}"
)


# In[56]:


inventory_invalid_products = (
    spark.table("silver_inventory")
    .join(
        spark.table("silver_products").select("ProductID"),
        on="ProductID",
        how="left_anti"
    )
)

inventory_invalid_stores = (
    spark.table("silver_inventory")
    .join(
        spark.table("silver_stores").select("StoreID"),
        on="StoreID",
        how="left_anti"
    )
)

print(
    f"Inventory with invalid ProductID: "
    f"{inventory_invalid_products.count():,}"
)

print(
    f"Inventory with invalid StoreID: "
    f"{inventory_invalid_stores.count():,}"
)


# In[57]:


returns_invalid_products = (
    spark.table("silver_returns")
    .join(
        spark.table("silver_products").select("ProductID"),
        on="ProductID",
        how="left_anti"
    )
)

returns_invalid_customers = (
    spark.table("silver_returns")
    .join(
        spark.table("silver_customers").select("CustomerID"),
        on="CustomerID",
        how="left_anti"
    )
)

print(
    f"Returns with invalid ProductID: "
    f"{returns_invalid_products.count():,}"
)

print(
    f"Returns with invalid CustomerID: "
    f"{returns_invalid_customers.count():,}"
)


# In[58]:


from pyspark.sql.functions import lit, current_timestamp

# Current trusted sales
df_sales_current = spark.table("silver_sales")

# Sales whose ProductID does not exist in silver_products
df_sales_bad_product = (
    df_sales_current
    .join(
        spark.table("silver_products").select("ProductID"),
        on="ProductID",
        how="left_anti"
    )
    .withColumn("RejectionReason", lit("INVALID_PRODUCT_REFERENCE"))
    .withColumn("QuarantinedAt", current_timestamp())
)

# Add them to the existing quarantine table
df_existing_quarantine_sales = spark.table("quarantine_sales")

df_updated_quarantine_sales = (
    df_existing_quarantine_sales
    .unionByName(
        df_sales_bad_product,
        allowMissingColumns=True
    )
)

df_updated_quarantine_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("quarantine_sales_final")

# Keep only sales whose ProductID exists in silver_products
df_sales_final = (
    df_sales_current
    .join(
        spark.table("silver_products").select("ProductID"),
        on="ProductID",
        how="left_semi"
    )
)

df_sales_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_sales_final")

print(f"Final Silver Sales: {df_sales_final.count():,}")
print(f"Additional Sales quarantined: {df_sales_bad_product.count():,}")


# In[59]:


df_inventory_current = spark.table("silver_inventory")

df_inventory_bad_product = (
    df_inventory_current
    .join(
        spark.table("silver_products").select("ProductID"),
        on="ProductID",
        how="left_anti"
    )
    .withColumn("RejectionReason", lit("INVALID_PRODUCT_REFERENCE"))
    .withColumn("QuarantinedAt", current_timestamp())
)

df_inventory_bad_product.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("quarantine_inventory")

df_inventory_final = (
    df_inventory_current
    .join(
        spark.table("silver_products").select("ProductID"),
        on="ProductID",
        how="left_semi"
    )
)

df_inventory_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_inventory_final")

print(f"Final Silver Inventory: {df_inventory_final.count():,}")
print(f"Inventory quarantined: {df_inventory_bad_product.count():,}")


# In[60]:


df_returns_current = spark.table("silver_returns")

# Invalid product references
df_returns_bad_product = (
    df_returns_current
    .join(
        spark.table("silver_products").select("ProductID"),
        on="ProductID",
        how="left_anti"
    )
    .withColumn("RejectionReason", lit("INVALID_PRODUCT_REFERENCE"))
    .withColumn("QuarantinedAt", current_timestamp())
)

# Invalid customer references
df_returns_bad_customer = (
    df_returns_current
    .join(
        spark.table("silver_customers").select("CustomerID"),
        on="CustomerID",
        how="left_anti"
    )
    .withColumn("RejectionReason", lit("INVALID_CUSTOMER_REFERENCE"))
    .withColumn("QuarantinedAt", current_timestamp())
)

# Combine rejected returns and avoid duplicate ReturnIDs
df_returns_quarantine = (
    df_returns_bad_product
    .unionByName(
        df_returns_bad_customer,
        allowMissingColumns=True
    )
    .dropDuplicates(["ReturnID"])
)

df_returns_quarantine.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("quarantine_returns")

# Remove rejected ReturnIDs
df_returns_final = (
    df_returns_current
    .join(
        df_returns_quarantine.select("ReturnID"),
        on="ReturnID",
        how="left_anti"
    )
)

df_returns_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_returns_final")

print(f"Final Silver Returns: {df_returns_final.count():,}")
print(f"Returns quarantined: {df_returns_quarantine.count():,}")


# In[61]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT 'Customers' AS Dataset, COUNT(*) AS RowCount
# FROM silver_customers

# UNION ALL

# SELECT 'Products', COUNT(*)
# FROM silver_products

# UNION ALL

# SELECT 'Stores', COUNT(*)
# FROM silver_stores

# UNION ALL

# SELECT 'Sales', COUNT(*)
# FROM silver_sales_final

# UNION ALL

# SELECT 'Returns', COUNT(*)
# FROM silver_returns_final

# UNION ALL

# SELECT 'Inventory', COUNT(*)
# FROM silver_inventory_final;

