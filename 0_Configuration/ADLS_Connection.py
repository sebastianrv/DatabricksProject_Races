# Databricks notebook source
# MAGIC %md ##Configuration
# MAGIC

# COMMAND ----------

storageAccountName = "sarsv2023"
storageAccountAccessKey = "4laHHRo3eLqlGttXboMIODFDxL85I+F8AitgF5cPS2HB+2CfzW8Wh8PKzscgVan/CFG8/i5unlWk+AStABkbNA=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-11-01T09:18:19Z&st=2023-10-27T01:18:19Z&spr=https&sig=Nbn2QYz0si6T7MNzx81UraySa4JGN9W57yGlc3oliEw%3D"
blobContainerName = "blob-raw"
mountPoint = "/mnt/sarsv2023/blob-raw"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

storageAccountName = "sarsv2023"
storageAccountAccessKey = "4laHHRo3eLqlGttXboMIODFDxL85I+F8AitgF5cPS2HB+2CfzW8Wh8PKzscgVan/CFG8/i5unlWk+AStABkbNA=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-11-01T09:18:19Z&st=2023-10-27T01:18:19Z&spr=https&sig=Nbn2QYz0si6T7MNzx81UraySa4JGN9W57yGlc3oliEw%3D"
blobContainerName = "blob-silver"
mountPoint = "/mnt/sarsv2023/blob-silver"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

storageAccountName = "sarsv2023"
storageAccountAccessKey = "4laHHRo3eLqlGttXboMIODFDxL85I+F8AitgF5cPS2HB+2CfzW8Wh8PKzscgVan/CFG8/i5unlWk+AStABkbNA=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-11-01T09:18:19Z&st=2023-10-27T01:18:19Z&spr=https&sig=Nbn2QYz0si6T7MNzx81UraySa4JGN9W57yGlc3oliEw%3D"
blobContainerName = "blob-gold"
mountPoint = "/mnt/sarsv2023/blob-gold"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)
