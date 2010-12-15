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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.avro.ipc.trace.TracePlugin.StorageType;
import org.junit.Test;

public class TestTraceConfigurations {

  @Test
  public void testTraceEquality() {
    TracePluginConfiguration conf1 = new TracePluginConfiguration();  
    TracePluginConfiguration conf2 = new TracePluginConfiguration();
    assertTrue(conf1.equals(conf2));
    assertEquals(conf1, conf2);
    
    TracePluginConfiguration conf3 = new TracePluginConfiguration();
    TracePluginConfiguration conf4 = new TracePluginConfiguration();
    
    conf3.traceProb = .12345;
    conf3.port = 333;
    conf3.clientPort = 444;
    conf3.storageType = StorageType.DISK;
    conf3.maxSpans = 10000;
    conf3.enabled = true;
    conf3.buffer = true;
    conf3.compressionLevel = 9;
    conf3.fileGranularitySeconds = 500;
    conf3.spanStorageDir = "/tmp";
    
    conf4.traceProb = .12345;
    conf4.port = 333;
    conf4.clientPort = 444;
    conf4.storageType = StorageType.DISK;
    conf4.maxSpans = 10000;
    conf4.enabled = true;
    conf4.buffer = true;
    conf4.compressionLevel = 9;
    conf4.fileGranularitySeconds = 500;
    conf4.spanStorageDir = "/tmp";
    
    assertTrue(conf3.equals(conf4));
    assertEquals(conf3, conf4);
    
    TracePluginConfiguration conf5 = new TracePluginConfiguration();
    conf5.traceProb = .4;
    assertTrue(!conf1.equals(conf5));
    
    conf5 = new TracePluginConfiguration();
    conf5.port = 333;
    assertTrue(!conf1.equals(conf5));
    
    conf5 = new TracePluginConfiguration();
    conf5.clientPort = 333;
    assertTrue(!conf1.equals(conf5));
    
    conf5 = new TracePluginConfiguration();
    conf5.storageType = StorageType.MEMORY;
    assertTrue(!conf1.equals(conf5));
    
    conf5 = new TracePluginConfiguration();
    conf5.maxSpans = 4;
    assertTrue(!conf1.equals(conf5));
    
    conf5 = new TracePluginConfiguration();
    conf5.enabled = false;
    assertTrue(!conf1.equals(conf5));
    
    conf5 = new TracePluginConfiguration();
    conf5.buffer = false;
    assertTrue(!conf1.equals(conf5));
    
    conf5 = new TracePluginConfiguration();
    conf5.compressionLevel = 0;
    assertTrue(!conf1.equals(conf5));
    
    conf5 = new TracePluginConfiguration();
    conf5.fileGranularitySeconds = 400;
    assertTrue(!conf1.equals(conf5));
    
    conf5 = new TracePluginConfiguration();
    conf5.spanStorageDir = "/foo";
    assertTrue(!conf1.equals(conf5));
  }
}
