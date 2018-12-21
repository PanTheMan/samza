package org.apache.samza.storage;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
  private FileSystem[] fileSystems = new FileSystem[4];
  private Path hdfsPath;
  private Path dbPath;
  public HDFSRestoreMetrics metrics;
  private ExecutorService restoreExecutor;

  public HDFSRestoreManager(Config config, String storePartitionPath, String storeName, MetricsRegistry registry) {
    HDFSBackupConfigs hdfsConfig = new HDFSBackupConfigs(config, storeName);

    metrics = new HDFSRestoreMetrics("restore", registry);
    LOGGER.info("Initialized HdfsRestoreManager: ");
    LOGGER.info("Store partition dir path: " + storePartitionPath);

    // Assumption that in jobs, last part of db dir is partition num
    String partitionNum = storePartitionPath.substring(storePartitionPath.lastIndexOf('/'));
    String HDFSPathStr = hdfsConfig.getBackupPath() + partitionNum;
    LOGGER.info("HDFS backup path: " + HDFSPathStr);

    // Set where to store backup files on hdfs
    hdfsPath = new Path(HDFSPathStr);

    // Initialize path variables
    dbPath = new Path(storePartitionPath);

    // Setup multithread copy
    restoreExecutor = Executors.newFixedThreadPool(hdfsConfig.getNumThreads(), new ThreadFactoryBuilder().setNameFormat("HDFSBackup-Copy-Task").setDaemon(true).build());


    // Setup fs configuration settings
    Configuration conf = new Configuration();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    conf.set(HDFSBackupConfigs.FS_DEFAULT_NAME, hdfsConfig.getDefaultName());
    // Possible IOException when using filesystem api
    try {
      // Set up filesystems for parallel copying
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
    FileSystem fs = fileSystems[0];

    // Find what files to copy from backup compare to what's in Rocksdb
    List<FileStatus> backupFilesToCopy = BackupUtil.getFilesOnlyInHDFS(fs, dbPath, hdfsPath);
    List<HDFSFileCopy> copyList = new ArrayList<>();

    LOGGER.info("Start parallel restore copying to store directory");
    int idx = 0;
    for (FileStatus backupFile : backupFilesToCopy) {
      copyList.add(new HDFSFileCopy(backupFile, dbPath, fileSystems[idx%4]));
      totalBytesCopied += backupFile.getLen();
      metrics.bytesRestored().inc(backupFile.getLen());
      idx++;
    }

    try {
      List<Future<Boolean>> futures = restoreExecutor.invokeAll(copyList);
      for (Future<Boolean> future : futures) {
        LOGGER.info(future.get().toString());
      }
    } catch (InterruptedException e) {
      throw new SamzaException("Error copying files in parallel using HDFS to: " + dbPath.toString(), e);
    } catch (ExecutionException e) {
      throw new SamzaException("Error copying files in parallel using HDFS to: " + dbPath.toString(), e);
    }
    LOGGER.info("Finished parallel restore copying to store directory");
    metrics.numFilesRestored().set(backupFilesToCopy.size());


    // Print statistics of restoring a backup
    long endTime = System.currentTimeMillis();
    long timeElapsed = endTime - startTime;
    metrics.storeRestoreTime().update(timeElapsed);
    LOGGER.info("Restoring RocksDB finished in milliseconds: " + timeElapsed + ", at: " + dbPath.toString());
    LOGGER.info("Copied " + backupFilesToCopy.size() + " files to rocksdb directory");
    LOGGER.info("Total size copied over: " + totalBytesCopied);

    // After delete the extra files in db that aren't in backup
    List<File> dbFilesToDelete = BackupUtil.getFilesOnlyInLocal(fs, dbPath, hdfsPath);
    for (File dbFile : dbFilesToDelete) {
      LOGGER.info("Delete file: " + dbFile.getName());
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

// Quick implementation of a Callable class for parallelized copying to and from HDFS
class HDFSFileCopy implements Callable<Boolean> {
  private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRestoreManager.class);
  private FileStatus file;
  private Path dbPath;
  private FileSystem fs;

  public HDFSFileCopy(FileStatus file, Path db, FileSystem fileSystem) {
    super();
    this.file = file;
    this.dbPath = db;
    this.fs = fileSystem;
  }

  @Override
  public Boolean call() {
    boolean isCompleted = false;
    try {
      LOGGER.info("Found file: " + file.getPath().getName());
      // Method: copyFromLocalFile(delSrc, overwrite, localFilePath, hdfsFilePath)
      fs.copyToLocalFile(file.getPath(), dbPath);
      LOGGER.info("Parallel restore finished of :" + file.getPath().getName());
      isCompleted = true;
    } catch (IOException e) {
      isCompleted = false;
      throw new SamzaException("Error copying files using HDFS to: " + dbPath.toString(), e);
    }

    return isCompleted;
  }

}
