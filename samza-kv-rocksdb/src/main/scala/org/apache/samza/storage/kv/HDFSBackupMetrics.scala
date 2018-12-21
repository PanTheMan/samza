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

package org.apache.samza.storage

import org.apache.samza.metrics.{MetricsHelper, MetricsRegistry, MetricsRegistryMap}

class HDFSBackupMetrics(
  val backupName: String = "unknown",
  val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {

  val backupKBPerSecRate = newGauge("backup-kb-per-sec-rate", 0.0)
  val deleteKBPerSecRate = newGauge("delete-kb-per-sec-rate", 0.0)
  val duplicateRate = newGauge("duplicate-rate", value=0.0)
  val totalBackupKBPerSecRate = newGauge("total-backup-kb-per-sec-rate", 0.0)
  val totalDeleteKBPerSecRate = newGauge("total-delete-kb-per-sec-rate", 0.0)


  val currentNumBackupFiles = newGauge("current-num-backup-files", 0)
  val currentNumDBFiles = newGauge("current-num-db-files", 0)
  val numFilesBackedUp = newGauge("num-files-backedup", 0)
  val numHDFSFilesDeleted = newGauge("num-hdfs-files-deleted", 0)
  val createBackupFilesMS = newTimer("create-backup-ms")
  val deleteBackupFilesMS = newTimer("delete-backup-ms")
  val backupCommitMS = newTimer("backup-commit-ms")
  val totalBytesBackedUp = newGauge("total-bytes-backedup", 0.0)
  val totalBytesDeleted = newGauge("total-bytes-deleted", 0.0)
  val bytesBackedUp = newCounter("bytes-backedup")
  val bytesDeleted = newCounter("bytes-deleted")
  val totalFilesBackedup = newGauge("total-files-backedup", 0)
  val totalFilesDeleted = newGauge("total-files-deleted", 0)
  val createCheckpointFilesMS = newTimer("create-checkpoint-ms")
  val deleteCheckpointFilesMS = newTimer("delete-checkpoint-ms")

  val totalTimeCreateBackupMS = newGauge("total-time-create-backup-ms",0.0)
  val totalTimeDeleteBackupMS = newGauge("total-time-delete-backup-ms",0.0)


  override def getPrefix = backupName + "-"
}
