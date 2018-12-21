package org.apache.samza.storage.kv;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.samza.SamzaException;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.storage.BackupUtil;
import org.apache.samza.storage.HDFSBackupConfigs;
import org.apache.samza.storage.HDFSBackupMetrics;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.samza.config.Config;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// Class to handle all operations relating to creating backups for RocksDB
public class HDFSBackupManager {
  // Variables for checkpoint info
  private Path checkpointPath;
  private Checkpoint checkpoint;
  private File checkpointFile;
  private File dbFile;
  private static final Logger LOGGER = LoggerFactory.getLogger(HDFSBackupManager.class);
  private FileSystem[] fileSystems = new FileSystem[4];
  private Path hdfsPath;
  private ExecutorService backupExecutor;
  public HDFSBackupMetrics metrics;
  // Stats to keep track of during all backups
  private long totalBytesCopied = 0;
  private long totalBytesDeleted = 0;
  private long totalFilesBackedup = 0;
  private long totalFilesDeleted = 0;
  private long totalTimeCreateBackup = 0;
  private long totalTimeDeleteBackup = 0;
  private long commitBottleneckCheckMS = 60000;


  public HDFSBackupManager(Config config, File dbDir, RocksDB db, MetricsRegistry registry) {
    HDFSBackupConfigs hdfsConfig = new HDFSBackupConfigs(config);
    dbFile = dbDir;
    metrics = new HDFSBackupMetrics("backup", registry);
    LOGGER.info("Initialize HDFSBackupManager: ");
    commitBottleneckCheckMS = hdfsConfig.getLong("task.commit.ms", commitBottleneckCheckMS);
    LOGGER.info("commit bottleneck is: " + commitBottleneckCheckMS);

    // Set where to store backup files on hdfs, with a unique path for each partition
    // Assumption that db path has the partition number at the end
    String dbDirStr = dbDir.toString();
    String partitionNum = dbDirStr.substring(dbDirStr.lastIndexOf('/'));
    LOGGER.info("Hdfs backup path: " + hdfsConfig.getBackupPath() + partitionNum);
    hdfsPath = new Path(hdfsConfig.getBackupPath() + partitionNum);

    // Setup fs configuration settings
    Configuration conf = new Configuration();
    conf.set(HDFSBackupConfigs.FS_DEFAULT_NAME, hdfsConfig.getDefaultName());
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    // Possible IOException when using filesystem api
    try {
      // Setup 4 fs for parallel copying
      fileSystems[0] = FileSystem.get(conf);
      fileSystems[1] = FileSystem.get(conf);
      fileSystems[2] = FileSystem.get(conf);
      fileSystems[3] = FileSystem.get(conf);

      // Create directory for hdfs backup location
      if (!fileSystems[0].mkdirs(hdfsPath)) {
        throw new SamzaException("Error creating backup in hdfs at path:" + hdfsPath.toString());
      }

    } catch(IOException e) {
      throw new SamzaException("Failed to create hdfs filesystem at : " + hdfsPath.toString(), e);
    }

    // Setup multi thread executor
    backupExecutor = Executors.newFixedThreadPool(hdfsConfig.getNumThreads(), new ThreadFactoryBuilder().setNameFormat("HDFSBackup-Copy-Task").setDaemon(true).build());

    // Setup RockDb's checkpoint info
    String checkpointPathStr = dbDirStr + "_checkpoint";
    LOGGER.info("Checkpoint path: " + checkpointPathStr);
    checkpoint = Checkpoint.create(db);
    checkpointPath = new Path(checkpointPathStr);
    checkpointFile =  new File(checkpointPathStr);
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
      metrics.createCheckpointFilesMS().update(timeElapsed);
      LOGGER.info("Time taken to create RockDb's checkpoint (ms): " + timeElapsed + ", at: " + checkpointFile.toString());
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
    metrics.deleteCheckpointFilesMS().update(timeElapsed);
    LOGGER.info("Time taken to delete RockDb's checkpoint (ms): " + timeElapsed);
  }

  // Method to create backup in hdfs
  public void createBackup() {
    LOGGER.info("Started creating backup now.");
    // TODO add flag to skip statistic tracking
    long startTime = System.currentTimeMillis();
    long bytesCopied = 0;
    long bytesDeleted = 0;
    long backupCommitTimeMS = 0;
    FileSystem fs = fileSystems[0];
    // Create a checkpoint
    createCheckpoint();

    // Get all files in checkpoint that are new / modified since last backup
    // (like manifest) and copy them to backup hdfs
    List<File> checkpointFilesToCopy = BackupUtil.getFilesOnlyInLocal(fs, checkpointPath, hdfsPath);

    try {
      for (File checkpointFile : checkpointFilesToCopy) {
        LOGGER.info("Found file: " + checkpointFile.getName());
        Path checkpointFilePath = new Path(checkpointFile.getPath());
        fs.copyFromLocalFile(false, true, checkpointFilePath, hdfsPath);
        bytesCopied += checkpointFile.length();
        metrics.bytesBackedUp().inc(checkpointFile.length());
      }
    } catch (IOException e) {
      throw new SamzaException(
          String.format("Error copying files using HDFS to: %s, from: %s", hdfsPath.toString(), checkpointPath), e);
    }
    //TODO: Here is the code for copying in parallel, commented out for now as some investigating needs to be done for subpar performance
    /*List<HDFSFileCopy> copyList = new ArrayList<>();
    int idx = 0;
    for (File checkpointFile : checkpointFilesToCopy) {
      copyList.add(new HDFSFileCopy(checkpointFile, hdfsPath, fileSystems[idx%4]));
      bytesCopied += checkpointFile.length();
      metrics.bytesBackedUp().inc(checkpointFile.length());
      idx++;
    }

    try {
      List<Future<Boolean>> futures = backupExecutor.invokeAll(copyList);
      for (Future<Boolean> future : futures) {
        if(!future.get())
          LOGGER.info("Something went wrong: Future returned false ");
      }
    } catch (InterruptedException e) {
      throw new SamzaException("Error copying files in parallel using HDFS to: " + hdfsPath.toString(), e);
    } catch (ExecutionException e) {
      throw new SamzaException("Error copying files in parallel using HDFS to: " + hdfsPath.toString(), e);
    }*/

    long timeElapsed = System.currentTimeMillis() - startTime;
    totalTimeCreateBackup += timeElapsed;
    backupCommitTimeMS += timeElapsed;
    float backupRate = (float) bytesCopied/timeElapsed;
    totalBytesCopied += bytesCopied;
    totalFilesBackedup += checkpointFilesToCopy.size();

    // Print statistics of creating a backup
    metrics.totalTimeCreateBackupMS().set(totalTimeCreateBackup);
    metrics.createBackupFilesMS().update(timeElapsed);
    metrics.numFilesBackedUp().set(checkpointFilesToCopy.size());
    metrics.totalFilesBackedup().set(totalFilesBackedup);
    metrics.totalBytesBackedUp().set(totalBytesCopied);
    metrics.backupKBPerSecRate().set(backupRate);
    LOGGER.info("Creating backup finished in ms: " + timeElapsed);
    LOGGER.info("Copied " + checkpointFilesToCopy.size() + " files to backup");
    LOGGER.info("File size copied over: " + bytesCopied);
    LOGGER.info("Backup rate per second" + backupRate);

    startTime = System.currentTimeMillis();
    // Delete unneeded files in backup compared to checkpoint
    List<FileStatus> backupFilesToDelete = BackupUtil.getFilesOnlyInHDFS(fs, checkpointPath, hdfsPath);

    try {
      for (FileStatus backupFile : backupFilesToDelete) {
        LOGGER.info("Delete file: " + backupFile.getPath().getName());
        // Method: delete(hdfsFilePath, recursive)
        fs.delete(backupFile.getPath(), true);
        bytesDeleted += backupFile.getLen();
        metrics.bytesDeleted().inc(backupFile.getLen());
      }
    } catch(IOException e) {
      throw new SamzaException("Error deleting files using HDFS from: " + hdfsPath.toString(), e);
    }

    timeElapsed = System.currentTimeMillis() - startTime;
    totalTimeDeleteBackup += timeElapsed;
    backupCommitTimeMS += timeElapsed;
    float deleteRate = (float) bytesDeleted/timeElapsed;
    totalBytesDeleted += bytesDeleted;
    totalFilesDeleted += backupFilesToDelete.size();

    // Print statistics of cleaning backup
    metrics.totalTimeDeleteBackupMS().set(timeElapsed);
    metrics.deleteBackupFilesMS().update(timeElapsed);
    metrics.numHDFSFilesDeleted().set(backupFilesToDelete.size());
    metrics.totalFilesDeleted().set(totalFilesDeleted);
    metrics.totalBytesDeleted().set(totalBytesDeleted);
    metrics.deleteKBPerSecRate().set(deleteRate);
    // Print statistics for deleting
    LOGGER.info("Deleting files in backup finished in ms: " + timeElapsed);
    LOGGER.info("Deleted " + backupFilesToDelete.size() + " files from backup directory: " + hdfsPath.toString());
    LOGGER.info("File size deleted: " + bytesDeleted);
    LOGGER.info("Delete rate per second: " + deleteRate);

    // Delete checkpoint directory
    deleteCheckpointDir();

    // Get logging info
    try {
      int backupFileNum = fs.listStatus(hdfsPath).length;
      int dbFileNum = dbFile.listFiles().length;
      float dupeRate = (float) totalBytesCopied/fs.getContentSummary(hdfsPath).getLength();
      float overallBackupRate = (float) totalBytesCopied/totalTimeCreateBackup;
      float overallDeleteRate = (float) totalBytesDeleted/totalTimeDeleteBackup;

      // Final statement
      LOGGER.info("Finished creating backup");
      LOGGER.info("Dupe rate: " + totalBytesCopied + " " + fs.getContentSummary(hdfsPath).getLength());
      LOGGER.info("Backup currently has " + backupFileNum + " files");
      LOGGER.info("DB currently has " + dbFileNum + " files");
      LOGGER.info("Total time spent creating a backup: " + totalTimeCreateBackup);
      LOGGER.info("Total time spent deleting in backup: " + totalTimeDeleteBackup);
      LOGGER.info("Total time spent committing this backup: " + backupCommitTimeMS);
      LOGGER.info("Overall backup rate: " + overallBackupRate);
      LOGGER.info("Overall delete rate: " + overallDeleteRate);

      metrics.duplicateRate().set(dupeRate);
      metrics.currentNumBackupFiles().set(backupFileNum);
      metrics.currentNumDBFiles().set(dbFileNum);
      metrics.totalBackupKBPerSecRate().set(overallBackupRate);
      metrics.totalDeleteKBPerSecRate().set(overallDeleteRate);
      metrics.backupCommitMS().update(backupCommitTimeMS);
      if(backupCommitTimeMS > commitBottleneckCheckMS) {
        LOGGER.info("BOTTLENECK COMMIT HERE!!!!");
      }
    } catch(IOException e) {
      throw new SamzaException("Error copying files using HDFS to: " + hdfsPath.toString(), e);
    }

  }
}

// Quick implementation of a Callable class for parallelized copying to and from HDFS
class HDFSFileCopy implements Callable<Boolean> {
  private static final Logger LOGGER = LoggerFactory.getLogger(HDFSBackupManager.class);
  private File file;
  private Path hdfsPath;
  private FileSystem fs;

  public HDFSFileCopy(File file, Path hdfs, FileSystem fileSystem) {
    super();
    this.file = file;
    this.hdfsPath = hdfs;
    this.fs = fileSystem;
  }

  @Override
  public Boolean call() {
    boolean isCompleted = false;
    try {
      LOGGER.info("Found file: " + file.getName());
      // Method: copyFromLocalFile(delSrc, overwrite, localFilePath, hdfsFilePath)
      Path pathToFile = new Path(file.getAbsolutePath());
      fs.copyFromLocalFile(false, true, pathToFile, hdfsPath);
      LOGGER.info("Parallel copy finished of :" + file.getName());
      isCompleted = true;
    } catch (IOException e) {
      isCompleted = false;
      throw new SamzaException("Error copying files using HDFS to: " + hdfsPath.toString(), e);
    }

    return isCompleted;
  }

}