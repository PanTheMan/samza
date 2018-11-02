package org.apache.samza.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;

// Class to hold common methods and variables for backup storage and restore process
public class StoreBackupEngine {
  protected static final Logger LOGGER = LoggerFactory.getLogger(StoreBackupEngine.class);
  protected FileSystem fs;
  protected Path hdfsPath;
  protected Path dbPath;
  public StoreBackupMetrics metrics;

  public StoreBackupEngine(Config storeConfig) {
    metrics = new StoreBackupMetrics("backup", new MetricsRegistryMap("registrymap"));

    // Setup fs configuration settings
    Configuration conf = new Configuration();
    // If core site path file is specified
    if (storeConfig.containsKey("hdfs.coreSitePath")){
      Path coreSitePath = new Path(storeConfig.get("hdfs.coreSitePath"));
      conf.addResource(coreSitePath);
    } else {
      // Otherwise, set a default localhost port for fs.defaultFS - hdfs://localhost:9000
      conf.set("fs.default.name", "hdfs://localhost:9000");
    }

    // Initialize path variables
    // Method: config.get(key, defaultVal)
    dbPath = new Path(storeConfig.get("rocksdb.dbDir", System.getProperty("java.io.tmpdir") + "/DB"));

    // Set where to store backup files on hdfs
    // TODO: change default value to something else for backup since obviously below default only works for me
    hdfsPath = new Path(storeConfig.get("hdfs.backupDir", "/User/eripan/backupTest/"));

    // Possible IOException when using filesystem api
    try {
      fs = FileSystem.get(conf);

      // Create directory for hdfs backup location
      // No need to check if localPath is dir since it's checked when rocksdb is created
      if (fs.exists(hdfsPath)) {
        if (fs.isFile(hdfsPath)) {
          throw new SamzaException("Error creating backup in hdfs: Path is already a file");
        }
      } else {
        fs.mkdirs(hdfsPath);
      }

    } catch(IOException e) {
      throw new SamzaException("Failed to create hdfs filesystem: " + e);
    }
  }

  // Method to restore rocksdb state from hdfs backup
  public Boolean restoreBackup() throws IOException {
    metrics.restores().inc();

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
    ArrayList<FileStatus> backupFilesToCopy = getNewFilesInBackupToLocal(dbPath);
    for (FileStatus backupFile : backupFilesToCopy) {
      LOGGER.info("Found file: " + backupFile.getPath().getName());
      totalBytesCopied += backupFile.getLen();
      // Method: copyToLocalFile(hdfsFilePath, localFilePath)
      fs.copyToLocalFile(backupFile.getPath(), dbPath);
    }

    metrics.bytesRestored().inc(totalBytesCopied);
    metrics.numFilesRestored().inc(backupFilesToCopy.size());
    // Print statistics of restoring a backup
    long endTime = System.currentTimeMillis();
    long timeElapsed = endTime - startTime;
    LOGGER.info("Restoring RocksDB finished in milliseconds: " + timeElapsed);
    LOGGER.info("Copied " + backupFilesToCopy.size() + " files to rocksdb directory");
    LOGGER.info("Total size copied over: " + totalBytesCopied);

    // After delete the extra files in db that aren't in backup
    ArrayList<File> dbFilesToDelete = getNewFilesInLocalToBackup(dbPath);
    for (File dbFile : dbFilesToDelete) {
      LOGGER.info("Found file to delete: " + dbFile.getName());
      totalBytesDeleted += dbFile.length();
      dbFile.delete();
    }

    // Print statistics
    metrics.numFilesDeleted().inc(dbFilesToDelete.size());
    metrics.bytesDeleted().inc(totalBytesDeleted);
    LOGGER.info("Deleted " + dbFilesToDelete.size() + " files from rocksdb directory");
    LOGGER.info("Total size deleted: " + totalBytesDeleted);

    return true;
  }


  // Method to find new files in backup compared to local
  public ArrayList getNewFilesInBackupToLocal(Path localPath) throws IOException {
    HashMap<String, File> localFileHmap = new HashMap<String, File>();
    ArrayList<FileStatus> filesNotInLocalList = new ArrayList<FileStatus>();
    // Store all the Rocksdb files in hashmap for quick comparison
    File dbDir = new File(localPath.toString());
    File[] filesList = dbDir.listFiles();
    for (File dbFile : filesList) {
      String fileName = dbFile.getName();
      localFileHmap.put(fileName, dbFile);
    }

    // Get all files in the hdfs backup and compare to local. Add if different/doesn't exist
    FileStatus[] backupFileIterator = fs.listStatus(hdfsPath);
    for (FileStatus backupFile : backupFileIterator) {
      String fileName = backupFile.getPath().getName();

      // If found locally, check filesize to see if needed
      if (localFileHmap.containsKey(fileName)) {
        // TODO: This may not be good enough check if things fail to copy or weird things happen, probably an edge case here to consider
        if (backupFile.getLen() > localFileHmap.get(fileName).length()) {
          filesNotInLocalList.add(backupFile);
        }
      // If not found, automatically add to list
      } else {
        filesNotInLocalList.add(backupFile);
      }
    }

    return filesNotInLocalList;
  }

  // Method to find new/Modified in local directory compared to backup
  public ArrayList<File> getNewFilesInLocalToBackup(Path localPath) throws IOException {
    HashMap<String, FileStatus> hdfsFileHmap = new HashMap<String, FileStatus>();
    ArrayList<File> filesNotInHDFSList = new ArrayList<File>();
    // Store all the backup files in hashmap for quick comparison
    FileStatus[] backupFileIterator = fs.listStatus(hdfsPath);
    for (FileStatus file : backupFileIterator) {
      String fileName = file.getPath().getName();
      hdfsFileHmap.put(fileName, file);
    }

    // Get all files in the local directory and compare to backup. Add if different/doesn't exist
    File localDir = new File(localPath.toString());
    File[] filesList = localDir.listFiles();
    for (File dbFile : filesList) {
      String fileName = dbFile.getName();

      if (hdfsFileHmap.containsKey(fileName)) {
        // TODO: This may not be good enough check if things fail to copy or weird things happen, probably an edge case here to consider
        // If found locally, check filesize to see if needed
        if (dbFile.length() > hdfsFileHmap.get(fileName).getLen()) {
          filesNotInHDFSList.add(dbFile);
        }
      } else {
        filesNotInHDFSList.add(dbFile);
      }
    }

    return filesNotInHDFSList;
  }

  // Method to close hdfs file system
  public void closeFileSystem() throws IOException {
    fs.close();
  }

}
