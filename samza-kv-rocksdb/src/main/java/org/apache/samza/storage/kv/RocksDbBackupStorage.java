package org.apache.samza.storage.kv;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.storage.StoreBackupEngine;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import java.util.HashMap;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.samza.config.Config;

// Class to handle all operations relating to creating backups for RocksDB
public class RocksDbBackupStorage extends StoreBackupEngine {
  // Variables for checkpoint info
  protected Path checkpointPath;
  protected Checkpoint checkpoint;
  protected File checkpointFile;

  public RocksDbBackupStorage(Config storeConfig, RocksDB db) {
    super(storeConfig);
    // Setup RockDb's checkpoint info
    checkpoint = Checkpoint.create(db);
    checkpointPath = new Path(storeConfig.get("rocksdb.checkpointDir", "/Users/eripan/checkpoint"));
    checkpointFile =  new File(checkpointPath.toString());
  }

  // Method to create RocksDb checkpoint
  private void createCheckpoint() {
    try {
      // Delete checkpoint dir if it already exists
      if(checkpointFile.exists())
        deleteCheckpointDir();

      // Track how much time to create rockdb checkpoint
      long startTime = System.currentTimeMillis();
      // Method: createCheckpoint(pathToCheckpointDir)
      checkpoint.createCheckpoint(checkpointPath.toString());
      long timeElapsed = System.currentTimeMillis() - startTime;
      LOGGER.info("Time taken to create RockDb's checkpoint: " + timeElapsed);
    } catch (RocksDBException e) {
      LOGGER.error("Can't create checkpoint for db at: " + checkpointPath.toString());
    }
  }

  // Method to delete RockDb's checkpoint
  private void deleteCheckpointDir() {
    long startTime = System.currentTimeMillis();

    // Delete rockdb's directory
    if (checkpointFile.exists()) {
      if (!FileUtil.fullyDelete(checkpointFile)) {
        throw new SamzaException("can not delete the checkpoint directory at: " + checkpointFile.getAbsolutePath());
      }
    }

    //Print time take for creating checkpoint
    long timeElapsed = System.currentTimeMillis() - startTime;
    LOGGER.info("Time taken to delete RockDb's checkpoint: " + timeElapsed);
  }

  // Method to create backup in hdfs
  public Boolean createBackup() throws IOException {
    metrics.backups().inc();

    // TODO add flag to skip statistic tracking
    long startTime = System.currentTimeMillis();
    long totalBytesCopied = 0;
    long totalBytesDeleted = 0;

    // Create a checkpoint
    createCheckpoint();
ba
    // Get all files in checkpoint that are new / modified since last backup
    // (like manifest) and copy them to backup hdfs
    ArrayList<File> checkpointFilesToCopy = getNewFilesInLocalToBackup(checkpointPath);
    for (File checkpointFile : checkpointFilesToCopy) {
      LOGGER.info("Found file: " + checkpointFile.getName());
      totalBytesCopied += checkpointFile.length();
      Path pathToFile = new Path(checkpointFile.getAbsolutePath());
      // Method: copyFromLocalFile(delSrc, overwrite, localFilePath, hdfsFilePath)
      fs.copyFromLocalFile(false, true, pathToFile, hdfsPath);
    }
    metrics.bytesBackedUp().inc(totalBytesCopied);
    metrics.numFilesBackedUp().inc(checkpointFilesToCopy.size());

    // Print statistics of creating a backup
    long timeElapsed = System.currentTimeMillis() - startTime;
    LOGGER.info("Creating backup finished in milliseconds: " + timeElapsed);
    LOGGER.info("Copied " + checkpointFilesToCopy.size() + " files to backup");
    LOGGER.info("Total size copied over: " + totalBytesCopied);

    // Delete unneeded files in backup compared to checkpoint
    ArrayList<FileStatus> backupFilesToDelete = getNewFilesInBackupToLocal(checkpointPath);
    for (FileStatus backupFile : backupFilesToDelete) {
      LOGGER.info("Found file: " + backupFile.getPath().getName());
      totalBytesDeleted += backupFile.getLen();
      // Method: delete(hdfsFilePath, recursive)
      fs.delete(backupFile.getPath(), true);
    }
    // Print statistics for deleting
    metrics.numFilesDeleted().inc(backupFilesToDelete.size());
    metrics.bytesDeleted().inc(totalBytesDeleted);
    LOGGER.info("Deleted " + backupFilesToDelete.size() + " files from backup directory");
    LOGGER.info("Total size deleted: " + totalBytesDeleted);

    // Delete checkpoint directory
    deleteCheckpointDir();
    return true;
  }
}
