package org.apache.samza.storage;

import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;


// Class to hold all configuration settings
public class HDFSBackupConfigs extends MapConfig {
  // Specifies localhost port for HDFS
  public static final String FS_DEFAULT_NAME= "fs.defaultFS";
  // Specifies where in hdfs to store backup files
  public static final String BACKUP_PATH = "backup.path";
  // Specifies how many threads to use in copying to and from hdfs
  public static final String BACKUP_THREAD_POOL_SIZE = "backup.thread.pool.size";
  // Field to keep track of possible prefix added to configs
  // Due to rocksdbkeyvalueStorage stripping prefixs
  private String storePrefix = "";

  public HDFSBackupConfigs(Map backupConfigs) {
    super(backupConfigs);
  }

  public HDFSBackupConfigs(Map backupConfigs, String storeName) {
    super(backupConfigs);
    storePrefix = String.format("stores.%s.", storeName);
  }

  public String getDefaultName() {
    if(!containsKey(storePrefix + FS_DEFAULT_NAME)) {
      throw new ConfigException("Missing required config: " + storePrefix + FS_DEFAULT_NAME);
    }

    return get(storePrefix + FS_DEFAULT_NAME);
  }

  public String getBackupPath() {
    if(!containsKey(storePrefix + BACKUP_PATH)) {
      throw new ConfigException("Missing required config: " + storePrefix + BACKUP_PATH);
    }
    return get(storePrefix + BACKUP_PATH);
  }

  public int getNumThreads() {

    if(!containsKey(storePrefix + BACKUP_THREAD_POOL_SIZE)) {
      throw new ConfigException("Missing required config: " + storePrefix + BACKUP_THREAD_POOL_SIZE);
    }
    return getInt(storePrefix + BACKUP_THREAD_POOL_SIZE);
  }
}
