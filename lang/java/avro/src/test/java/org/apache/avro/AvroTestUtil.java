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
package org.apache.avro;

import java.io.File;

/** Utilities for Avro tests. */
public class AvroTestUtil {
  static final File TMPDIR = new File(System.getProperty("test.dir", System.getProperty("java.io.tmpdir", "/tmp")), "tmpfiles");

  private AvroTestUtil() {
  }

  /** 
   * Create a temporary file in a test-appropriate directory.
   * 
   * @param testClass The test case class requesting the file creation
   * @param name The name of the file to be created 
   */
  public static File tempFile(Class testClass, String name) {
    File testClassDir = new File(TMPDIR, testClass.getName());
    testClassDir.mkdirs();
    return new File(testClassDir, name);
  }

  /** 
   * Create a temporary directory in a test-appropriate directory.
   * 
   * @param testClass The test case class requesting the directory creation
   * @param name The name of the directory to be created  
   */
  public static File tempDirectory(Class testClass, String name) {
    File tmpFile = tempFile(testClass, name);
    tmpFile.delete();
    tmpFile.mkdir();
    return tmpFile;
  }

}
