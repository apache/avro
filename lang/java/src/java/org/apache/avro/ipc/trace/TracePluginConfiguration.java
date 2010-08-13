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
package org.apache.avro.ipc.trace;

import org.apache.avro.ipc.trace.TracePlugin.StorageType;

/**
 * Helper class for configuring Avro's {@link TracePlugin}. If you are using 
 * a common configuration module, wrap this class with your own configuration. 
 */
public class TracePluginConfiguration {
  public double traceProb; // Probability of starting tracing
  public int port;         // Port to serve tracing data
  public int clientPort;   // Port to expose client HTTP interface
  public StorageType storageType;  // How to store spans
  public long maxSpans;   // Max number of spans to store
  public boolean enabled; // Whether or not we are active
  public boolean buffer;  // If disk storage, whether to buffer writes
  public int compressionLevel; // If using file storage, what compression
                               // level (0-9).
  public int fileGranularitySeconds; // How many seconds of span data to store
                                     // in each file.
  public String spanStorageDir; // where to store span data, if file-based
  
  /**
   * Return a TracePluginConfiguration with default options.
   */
  public TracePluginConfiguration() {
    this.traceProb = 0.0;
    this.port = 12335;
    this.clientPort = 12345;
    this.storageType = StorageType.MEMORY;
    this.maxSpans = 10000;
    this.enabled = true;
    this.buffer = true;
    this.compressionLevel = 9;
    this.fileGranularitySeconds = 500;
    this.spanStorageDir = "/tmp";
  }
}
