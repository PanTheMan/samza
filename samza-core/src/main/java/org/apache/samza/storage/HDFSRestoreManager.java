package org.apache.samza.storage;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HDFSRestoreManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRestoreManager.class);
  private FileSystem fs;
  private Path hdfsPath;
  private Path dbPath;
  public HDFSRestoreMetrics metrics;

  public HDFSRestoreManager(Config config, String dbDirPath, MetricsRegistry registry) {
    HDFSConfigs hdfsConfig = new HDFSConfigs(config);

    metrics = new HDFSRestoreMetrics("backup", registry);

    // Set where to store backup files on hdfs
    // TODO: change default value to something else for backup since obviously below default only works for me
    hdfsPath = new Path(hdfsConfig.getBackupPath());

    // Initialize path variables
    dbPath = new Path(dbDirPath);

    // Setup fs configuration settings
    Configuration conf = new Configuration();
    conf.set(HDFSConfigs.FS_DEFAULT_NAME, hdfsConfig.getDefaultName());
    // Possible IOException when using filesystem api
    try {
      fs = FileSystem.get(conf);

      // Create directory for hdfs backup location
      if (!fs.mkdirs(hdfsPath)) {
        throw new SamzaException("Error creating backup in hdfs at path:" + hdfsPath.toString());
      }

    } catch(IOException e) {
      throw new SamzaException("Failed to create hdfs filesystem at : " + hdfsPath.toString(), e);
    }

  }

  // Method to restore state from hdfs backup
  public void restore() throws IOException {
    // Check if db directory has already been created or not
    File dbDir = new File(dbPath.toString());
    if(!dbDir.exists()) {
      dbDir.mkdirs();
    }

    // Variables to keep track of statistics
    long startTime = System.currentTimeMillis();
    long totalBytesCopied = 0;
    long totalBytesDeleted = 0;

    // Find what files to copy from backup compare to what's in Rocksdb
    List<FileStatus> backupFilesToCopy = BackupUtil.getFilesOnlyInHDFS(fs, dbPath, hdfsPath);
    for (FileStatus backupFile : backupFilesToCopy) {
      LOGGER.info("Found file: " + backupFile.getPath().getName());
      // Method: copyToLocalFile(hdfsFilePath, localFilePath)
      fs.copyToLocalFile(backupFile.getPath(), dbPath);
      totalBytesCopied += backupFile.getLen();
      metrics.bytesRestored().inc(backupFile.getLen());
    }
    metrics.numFilesRestored().set(backupFilesToCopy.size());


    // Print statistics of restoring a backup
    long endTime = System.currentTimeMillis();
    long timeElapsed = endTime - startTime;
    metrics.totalTimeRestore().update(timeElapsed);
    LOGGER.info("Restoring RocksDB finished in milliseconds: " + timeElapsed + ", at: " + dbPath.toString());
    LOGGER.info("Copied " + backupFilesToCopy.size() + " files to rocksdb directory");
    LOGGER.info("Total size copied over: " + totalBytesCopied);

    // After delete the extra files in db that aren't in backup
    List<File> dbFilesToDelete = BackupUtil.getFilesOnlyInLocal(fs, dbPath, hdfsPath);
    for (File dbFile : dbFilesToDelete) {
      LOGGER.info("Found file to delete: " + dbFile.getName());
      dbFile.delete();
      totalBytesDeleted += dbFile.length();
      metrics.bytesDeleted().inc(dbFile.length());
    }
    metrics.numFilesDeleted().set(dbFilesToDelete.size());


    // Print statistics
    long timeToCleanDir = System.currentTimeMillis();
    timeElapsed = timeToCleanDir - endTime;
    metrics.totalTimeCleaningLocalDir().update(timeElapsed);
    LOGGER.info("Time to clean up local directory: " + timeElapsed);
    LOGGER.info("Deleted " + dbFilesToDelete.size() + " files from store directory: " + dbPath.toString());
    LOGGER.info("Total size deleted: " + totalBytesDeleted);
  }
}
