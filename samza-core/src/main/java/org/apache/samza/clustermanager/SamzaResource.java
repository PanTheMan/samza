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

package org.apache.samza.clustermanager;

import com.google.common.annotations.VisibleForTesting;


/**
 * Specification of a Samza Resource. A resource is identified by a unique resource ID.
 * A resource is currently comprised of CPUs and Memory resources on a host.
 */
public class SamzaResource {
  private final int numCores;
  private final int memoryMb;
  private final String host;
  private final String containerId;
  private final long timestamp;

  //TODO: Investigate adding disk space. Mesos supports disk based reservations.

  public SamzaResource(int numCores, int memoryMb, String host, String containerId) {
    this.numCores = numCores;
    this.memoryMb = memoryMb;
    this.host = host;
    this.containerId = containerId;
    this.timestamp = System.currentTimeMillis();
  }

  @VisibleForTesting
  SamzaResource(int numCores, int memoryMb, String host, String containerId, long timestamp) {
    this.numCores = numCores;
    this.memoryMb = memoryMb;
    this.host = host;
    this.containerId = containerId;
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SamzaResource resource = (SamzaResource) o;

    if (numCores != resource.numCores) return false;
    if (memoryMb != resource.memoryMb) return false;
    return containerId.equals(resource.containerId);

  }

  @Override
  public int hashCode() {
    int result = numCores;
    result = 31 * result + memoryMb;
    result = 31 * result + containerId.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "SamzaResource{" +
        "host='" + host + '\'' +
        ", containerId='" + containerId + '\'' +
        '}';
  }

  public int getNumCores() {
    return numCores;
  }

  public int getMemoryMb() {
    return memoryMb;
  }

  public String getHost() {
    return host;
  }

  public String getContainerId() {
    return containerId;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
