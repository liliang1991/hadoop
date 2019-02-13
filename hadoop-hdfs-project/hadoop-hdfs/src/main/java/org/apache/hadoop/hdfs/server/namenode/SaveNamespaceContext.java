/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileNamesystem;
import org.apache.hadoop.hdfs.util.Canceler;

import com.google.common.base.Preconditions;

/**
 * Context for an ongoing SaveNamespace operation. This class
 * allows cancellation, and also is responsible for accumulating
 * failed storage directories.
 */
public class SaveNamespaceContext {
   FSNamesystem sourceNamesystem;
    FileNamesystem fileNamesystem;
    private final long txid;
  private final List<StorageDirectory> errorSDs =
    Collections.synchronizedList(new ArrayList<StorageDirectory>());
  
  private final Canceler canceller;
  private CountDownLatch completionLatch = new CountDownLatch(1);
  public SaveNamespaceContext(
      FSNamesystem sourceNamesystem,
      long txid,
      Canceler canceller) {
    this.sourceNamesystem = sourceNamesystem;
    this.txid = txid;
    this.canceller = canceller;
  }
  public SaveNamespaceContext(
          FileNamesystem sourceNamesystem,
          long txid,
          Canceler canceller) {
    this.fileNamesystem = sourceNamesystem;
    this.txid = txid;
    this.canceller = canceller;
  }
  FSNamesystem getSourceNamesystem() {
    return sourceNamesystem;
  }
  public   FileNamesystem getFileSourceNamesystem() {
        return fileNamesystem;
    }
  public long getTxId() {
    return txid;
  }

 public void reportErrorOnStorageDirectory(StorageDirectory sd) {
    errorSDs.add(sd);
  }

public   List<StorageDirectory> getErrorSDs() {
    return errorSDs;
  }

 public void markComplete() {
    Preconditions.checkState(completionLatch.getCount() == 1,
        "Context already completed!");
    completionLatch.countDown();
  }

 public void checkCancelled() throws SaveNamespaceCancelledException {
    if (canceller.isCancelled()) {
      throw new SaveNamespaceCancelledException(
          canceller.getCancellationReason());
    }
  }
}
