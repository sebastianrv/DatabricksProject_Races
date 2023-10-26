# Databricks notebook source
# MAGIC %md ##Configuration
# MAGIC

# COMMAND ----------

storageAccountName = "sagrupo3proyecto1"
storageAccountAccessKey = "dCqFrsx23kMp40BMATH/iyWqyHQ396vVGDMkh0M0YhKOSnguPHMXWx81FLRdrTjbqPlGM5wZjhTB+AStT8JcMw=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-11-02T11:48:38Z&st=2023-10-26T03:48:38Z&spr=https&sig=O82lWvvnr2Q9xKE8Cmv3PhrLk%2Bj19EzVjVRcj29DpPk%3D"
blobContainerName = "p1-raw"
mountPoint = "/mnt/sagrupo3proyecto1/p1-raw"
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
