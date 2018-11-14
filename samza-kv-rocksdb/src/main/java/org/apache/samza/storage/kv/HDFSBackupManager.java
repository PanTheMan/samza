package org.apache.samza.storage.kv;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.storage.BackupUtil;
import org.apache.samza.storage.HDFSConfigs;
import org.apache.samza.storage.HDFSBackupMetrics;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.samza.config.Config;

// Class to handle all operations relating to creating backups for RocksDB
public class HDFSBackupManager {
  // Variables for checkpoint info
  private Path checkpointPath;
  private Checkpoint checkpoint;
  private File checkpointFile;
  private static final Logger LOGGER = LoggerFactory.getLogger(HDFSBackupManager.class);
  private FileSystem fs;
  private Path hdfsPath;
  public HDFSBackupMetrics metrics;

  public HDFSBackupManager(Config config, RocksDB db, MetricsRegistry registry) {
    HDFSConfigs hdfsConfig = new HDFSConfigs(config);

    metrics = new HDFSBackupMetrics("backup", registry);

    // Set where to store backup files on hdfs
    // TODO: change default value to something else for backup since obviously below default only works for me
    hdfsPath = new Path(hdfsConfig.getBackupPath());

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
    // Setup RockDb's checkpoint info
    checkpoint = Checkpoint.create(db);
    checkpointPath = new Path(hdfsConfig.getCheckpointPath());
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
      metrics.totalTimeCreateCheckpoint().update(timeElapsed);
      LOGGER.info("Time taken to create RockDb's checkpoint: " + timeElapsed + ", at: " + checkpointFile.toString());
    } catch (RocksDBException e) {
      LOGGER.error("Can't create checkpoint for db at: " + checkpointPath.toString(), e);
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
  public void createBackup() {
    // TODO add flag to skip statistic tracking
    long startTime = System.currentTimeMillis();
    long totalBytesCopied = 0;
    long totalBytesDeleted = 0;

    // Create a checkpoint
    createCheckpoint();

    // Get all files in checkpoint that are new / modified since last backup
    // (like manifest) and copy them to backup hdfs
    List<File> checkpointFilesToCopy = BackupUtil.getFilesOnlyInLocal(fs, checkpointPath, hdfsPath);

    try {
      for (File checkpointFile : checkpointFilesToCopy) {
        LOGGER.info("Found file: " + checkpointFile.getName());
        Path pathToFile = new Path(checkpointFile.getAbsolutePath());
        // Method: copyFromLocalFile(delSrc, overwrite, localFilePath, hdfsFilePath)
        fs.copyFromLocalFile(false, true, pathToFile, hdfsPath);
        totalBytesCopied += checkpointFile.length();
        metrics.bytesBackedUp().inc(checkpointFile.length());
      }
    } catch(IOException e) {
      throw new SamzaException("Error copying files using HDFS to: " + hdfsPath.toString(), e);
    }

    metrics.numFilesBackedUp().set(checkpointFilesToCopy.size());
    // Print statistics of creating a backup
    long timeElapsed = System.currentTimeMillis() - startTime;
    metrics.totalTimeCreateBackup().update(timeElapsed);
    LOGGER.info("Creating backup finished in milliseconds: " + timeElapsed);
    LOGGER.info("Copied " + checkpointFilesToCopy.size() + " files to backup");
    LOGGER.info("Total size copied over: " + totalBytesCopied);

    // Delete unneeded files in backup compared to checkpoint
    List<FileStatus> backupFilesToDelete = BackupUtil.getFilesOnlyInHDFS(fs, checkpointPath, hdfsPath);

    try {
      for (FileStatus backupFile : backupFilesToDelete) {
        LOGGER.info("Found file: " + backupFile.getPath().getName());
        // Method: delete(hdfsFilePath, recursive)
        fs.delete(backupFile.getPath(), true);
        totalBytesDeleted += backupFile.getLen();
        metrics.bytesDeleted().inc(backupFile.getLen());
      }
    } catch(IOException e) {
      throw new SamzaException("Error deleting files using HDFS from: " + hdfsPath.toString(), e);
    }

    metrics.numFilesDeleted().set(backupFilesToDelete.size());
    // Print statistics for deleting
    LOGGER.info("Deleted " + backupFilesToDelete.size() + " files from backup directory: " + hdfsPath.toString());
    LOGGER.info("Total size deleted: " + totalBytesDeleted);

    // Delete checkpoint directory
    deleteCheckpointDir();
  }
}
