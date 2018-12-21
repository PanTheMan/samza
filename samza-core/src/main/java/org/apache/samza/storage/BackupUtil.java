package org.apache.samza.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
public final class BackupUtil {

  // Method to find new files in backup compared to local
  public static List<FileStatus> getFilesOnlyInHDFS(FileSystem fs, Path localPath, Path hdfsPath) {
    HashMap<String, File> localFiles = new HashMap<String, File>();
    ArrayList<FileStatus> filesOnlyInHDFS = new ArrayList<FileStatus>();
    // Store all the Rocksdb files in hashmap for quick comparison
    File dbDir = new File(localPath.toString());
    File[] files = dbDir.listFiles();
    for (File dbFile : files) {
      String fileName = dbFile.getName();
      localFiles.put(fileName, dbFile);
    }

    // Get all files in the hdfs backup and compare to local. Add if different/doesn't exist
    FileStatus[] backupIterator;
    try {
      backupIterator = fs.listStatus(hdfsPath);
    } catch(IOException e) {
      throw new SamzaException("Error listing all files in HDFS at: " + hdfsPath, e);
    }
    for (FileStatus backupFile : backupIterator) {
      String fileName = backupFile.getPath().getName();

      // If not found locally, add file without checking size
      if (!localFiles.containsKey(fileName)) {
        filesOnlyInHDFS.add(backupFile);
      } else {
        if (backupFile.getLen() > localFiles.get(fileName).length()) {
          filesOnlyInHDFS.add(backupFile);
        }
      }
    }

    return filesOnlyInHDFS;
  }

  // Method to find new/Modified in local directory compared to backup
  public static List<File> getFilesOnlyInLocal(FileSystem fs, Path localPath, Path hdfsPath) {
    HashMap<String, FileStatus> hdfsFiles = new HashMap<String, FileStatus>();
    ArrayList<File> filesOnlyInLocal = new ArrayList<File>();
    // Store all the backup files in hashmap for quick comparison
    FileStatus[] backupIterator;
    try {
      backupIterator = fs.listStatus(hdfsPath);
    } catch(IOException e) {
      throw new SamzaException("Error listing all files in HDFS at: " + hdfsPath, e);
    }

    for (FileStatus file : backupIterator) {
      String fileName = file.getPath().getName();
      hdfsFiles.put(fileName, file);
    }

    // Get all files in the local directory and compare to backup. Add if different/doesn't exist
    File localDir = new File(localPath.toString());
    File[] files = localDir.listFiles();
    for (File dbFile : files) {
      String fileName = dbFile.getName();

      // If not found locally, add file without checking size
      if (!hdfsFiles.containsKey(fileName)) {
        filesOnlyInLocal.add(dbFile);
      } else {
        // TODO: This may not be good enough check if things fail to copy or weird things happen, probably an edge case here to consider
        if (dbFile.length() > hdfsFiles.get(fileName).getLen()) {
          filesOnlyInLocal.add(dbFile);
        }
      }
    }

    return filesOnlyInLocal;
  }
}
