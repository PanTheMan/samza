package org.apache.samza.storage;

import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;


// Class to hold all configuration settings
public class HDFSConfigs extends MapConfig {
  // Specifies localhost port for HDFS
  public static final String FS_DEFAULT_NAME= "fs.default.name";
  // Specifies where in hdfs to store backup files
  public static final String BACKUP_PATH = "backup.path";

  public static final String CHECKPOINT_PATH = "checkpoint.path";

  public HDFSConfigs(Map backupConfigs) {
    super(backupConfigs);
  }

  public String getDefaultName() {
    if(!containsKey(FS_DEFAULT_NAME)) {
      throw new ConfigException("Missing required config: " + FS_DEFAULT_NAME);
    }

    return get(FS_DEFAULT_NAME);
  }

  public String getBackupPath() {
    if(!containsKey(BACKUP_PATH)) {
      throw new ConfigException("Missing required config: " + BACKUP_PATH);
    }
    return get(BACKUP_PATH);
  }

  public String getCheckpointPath() {
    if(!containsKey(CHECKPOINT_PATH)) {
      throw new ConfigException("Missing required config: " + CHECKPOINT_PATH);
    }
    return get(CHECKPOINT_PATH);
  }
}
