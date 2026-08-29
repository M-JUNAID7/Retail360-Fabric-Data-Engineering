#!/usr/bin/env python
# coding: utf-8

# ## 01_Bronze_Ingestion
# 
# null

# In[ ]:


# Welcome to your new notebook
# Type here in the cell editor to add code!


# In[1]:


df_customers = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("Files/bronze/customers/customers.csv")

display(df_customers.limit(10))


# In[2]:


df_customers.printSchema()


# In[3]:


df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_customers")


# In[5]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT COUNT(*) AS CustomerCount
# FROM bronze_customers;


# In[7]:


datasets = {
    "products": "Files/bronze/products/products.csv",
    "stores": "Files/bronze/stores/stores.csv",
    "sales": "Files/bronze/sales/sales.csv",
    "returns": "Files/bronze/returns/returns.csv",
    "inventory": "Files/bronze/inventory/inventory.csv"
}

print("Datasets configured:", len(datasets))


# In[8]:


def ingest_to_bronze(dataset_name, file_path):
    
    print(f"Starting ingestion: {dataset_name}")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )
    
    table_name = f"bronze_{dataset_name}"
    
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
    
    row_count = spark.table(table_name).count()
    
    print(f"Completed: {table_name}")
    print(f"Rows loaded: {row_count:,}")


# In[9]:


for dataset_name, file_path in datasets.items():
    ingest_to_bronze(dataset_name, file_path)


# In[10]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT
#     'customers' AS Dataset,
#     COUNT(*) AS RowCount
# FROM bronze_customers

# UNION ALL

# SELECT
#     'products',
#     COUNT(*)
# FROM bronze_products

# UNION ALL

# SELECT
#     'stores',
#     COUNT(*)
# FROM bronze_stores

# UNION ALL

# SELECT
#     'sales',
#     COUNT(*)
# FROM bronze_sales

# UNION ALL

# SELECT
#     'returns',
#     COUNT(*)
# FROM bronze_returns

# UNION ALL

# SELECT
#     'inventory',
#     COUNT(*)
# FROM bronze_inventory;

