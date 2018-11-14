/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.storage.kv;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.storage.BackupUtil;
import org.apache.samza.storage.HDFSConfigs;
import org.apache.samza.storage.HDFSRestoreManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

public class TestHDFSBackupManager {
  // Set testing values for storage locations
  private static final String DB_NAME = "testRocksDbStore";
  private static String dbStrPath = System.getProperty("java.io.tmpdir") + "/DB";
  private static File dbFile = new File(dbStrPath);
  private static String backupStrPath = "/Users/eripan/backup";
  private static Path backupPath = new Path(backupStrPath);
  private static String checkpointStrPath = "/Users/eripan/checkpoint";
  private static String hdfsDefaultName = "hdfs://localhost:9000";
  private static FileSystem fs = null;
  private static Config config;
  private static MetricsRegistry metricsReg = new MetricsRegistryMap("test");

  static private RocksDbKeyValueStore createStore() {
    // Configs for setting up backup in RocksdbKVStore
    HashMap<String, String> map = new HashMap<String, String>();
    map.put(HDFSConfigs.CHECKPOINT_PATH, checkpointStrPath);
    map.put(HDFSConfigs.BACKUP_PATH, backupStrPath);
    map.put(HDFSConfigs.FS_DEFAULT_NAME, hdfsDefaultName);
    config = new MapConfig(map);
    Options options = new Options().setCreateIfMissing(true);
    return new RocksDbKeyValueStore(dbFile, options, config, false, DB_NAME,
        new WriteOptions(), new FlushOptions(), new KeyValueStoreMetrics("dbStore", metricsReg));
  }

  @BeforeClass
  static public void setup() throws IOException {
    // Set up FileSystem for HDFS commands
    Configuration conf = new Configuration();
    conf.set(HDFSConfigs.FS_DEFAULT_NAME, hdfsDefaultName);
    fs = FileSystem.get(conf);
  }

  @Test
  public void testFlushBackup() {
    RocksDbKeyValueStore store = createStore();
    // Flush some data into rocksdb
    byte[] key = "key".getBytes();
    store.put(key, "val".getBytes());
    store.flush();

    File checkpointDir = new File(checkpointStrPath);
    // After test, check checkpoint location is cleaned up
    Assert.assertTrue(!checkpointDir.exists());
    HashMap<String, File> fileMap = new HashMap<String, File>();
    for(File file : dbFile.listFiles()) {
      fileMap.put(file.getName(), file);
    }

    // Check if hdfs backup location has the files needed
    try {
      Assert.assertTrue(fs.listStatus(backupPath).length != 0);
      for(FileStatus status : fs.listStatus(backupPath)) {
        Assert.assertTrue(fileMap.containsKey(status.getPath().getName()));
      }
      fs.delete(backupPath,true);
    } catch(IOException e) {
      throw new SamzaException("Error occurred while trying to use hdfs filesystem: " + backupPath, e);
    }
    tearDown(store);
  }

  @Test
  public void testRestoreBackup() {
    RocksDbKeyValueStore store = createStore();
    byte[] key = "key".getBytes();
    byte[] val = "val".getBytes();
    store.put(key, val);
    store.flush();

    tearDown(store);

    try {
      // Check if the key value is there after restore
      HDFSRestoreManager restoreTest = new HDFSRestoreManager(config, dbStrPath, metricsReg);
      restoreTest.restore();
      store = createStore();
      store.put("testest".getBytes(), key);
      Assert.assertEquals("val", new String(store.get(key)));
      // Clean backup dir
      fs.delete(backupPath,true);
    } catch(IOException e) {
      throw new SamzaException("Error occurred while trying to use hdfs filesystem: " + e);
    }
    tearDown(store);
  }

  // Method to tear down rocksdb store
  static public void tearDown(RocksDbKeyValueStore store) {
    if (store != null) {
      store.close();
    }
  }

}
