# Databricks notebook source
# DBTITLE 1,Prerequisites
# MAGIC %md
# MAGIC 
# MAGIC 1. Create Service principle 
# MAGIC 2. Assign grant read/write permission to Service principle on storage account

# COMMAND ----------

# DBTITLE 1,Check existing mounts
dbutils.fs.mounts()

# COMMAND ----------

# DBTITLE 1,Mount ADLS to databricks workspace
try: 
    tenantId = "cf9aaabc-afbf-4f8b-8d0a-f63315d70bf2"
    containerName = "container01"
    storageAccountName = "strvarun01"
    mountPoint = "/mnt/mt_"+ containerName
    
    
    configs = {"fs.azure.account.auth.type":"OAuth",
              "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id":"3a03f54d-6b2e-40d7-85fe-e223fcb1987a",
              "fs.azure.account.oauth2.client.secret":"lAV8Q~3SDd.kZybP4LU3bWM25GoxHf1wbmJ4GcCI",
               "fs.azure.account.oauth2.client.endpoint":"https://login.microsoftonline.com/{0}/oauth2/token".format(tenantId)}
     
    print(mountPoint)
    
    if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
        print('Varun')
        dbutils.fs.mount(
            source = "abfss://{0}@{1}.dfs.core.windows.net/".format(containerName,storageAccountName),
            mount_point = mountPoint,
            extra_configs = configs)

    
except Exception as inst:
        print (inst)
    

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.help()

# COMMAND ----------

path = dbutils.fs.ls(mountPoint+'/dataset')[0].path

# COMMAND ----------

# DBTITLE 1,Read CSV file from Azure blob storage and load in PySpark DataFrame 
#df = spark.read.csv(path, header = True)



df = spark.read.format('csv').option('header', 'true').load(path)

display(df)

# COMMAND ----------

# DBTITLE 1,Create Databricks Delta table and storage in Azure blob storage
path = mountPoint+'/output1'
print(path)
df.write\
.format('delta')\
.partitionBy('variety')\
.mode('  ')\
.save(path+'/delta_iris')

# COMMAND ----------

# DBTITLE 1,Read Databricks Delta table from  Azure blob storage
read_delta_df =  spark\
                .read\
                .format('delta')\
                .load(path+'/delta_iris/')

display(read_delta_df)

# COMMAND ----------

#pip install delta-spark

# COMMAND ----------

from delta.tables import *  

# COMMAND ----------

dt = DeltaTable.forPath(spark,path+'/delta_iris')

# COMMAND ----------

display(dt.history())

# COMMAND ----------

