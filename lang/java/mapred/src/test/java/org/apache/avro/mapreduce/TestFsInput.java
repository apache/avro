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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.avro.mapreduce;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import org.apache.avro.AvroTestUtil;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFsInput {
  private static File file;
  private static final String FILE_CONTENTS = "abcdefghijklmnopqrstuvwxyz";
  private Configuration conf;
  private FsInput fsInput;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    File directory = AvroTestUtil.tempDirectory(TestFsInput.class, "file");
    file = new File(directory, "file.txt");
    PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), Charset.forName("UTF-8")));
    try {
      out.print(FILE_CONTENTS);
    } finally {
      out.close();
    }
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    fsInput = new FsInput(new Path(file.getPath()), conf);
  }

  @After
  public void tearDown() throws Exception {
    if (fsInput != null) {
      fsInput.close();
    }
  }

  @Test
  public void testConfigurationConstructor() throws Exception {
    FsInput in = new FsInput(new Path(file.getPath()), conf);
    try {
      int expectedByteCount = 1;
      byte[] readBytes = new byte[expectedByteCount];
      int actualByteCount = fsInput.read(readBytes, 0, expectedByteCount);
      assertThat(actualByteCount, is(equalTo(expectedByteCount)));
    } finally {
      in.close();
    }
  }

  @Test
  public void testFileSystemConstructor() throws Exception {
    Path path = new Path(file.getPath());
    FileSystem fs = path.getFileSystem(conf);
    FsInput in = new FsInput(path, fs);
    try {
      int expectedByteCount = 1;
      byte[] readBytes = new byte[expectedByteCount];
      int actualByteCount = fsInput.read(readBytes, 0, expectedByteCount);
      assertThat(actualByteCount, is(equalTo(expectedByteCount)));
    } finally {
      in.close();
    }
  }

  @Test
  public void testLength() throws IOException {
    assertEquals(fsInput.length(), FILE_CONTENTS.length());
  }

  @Test
  public void testRead() throws Exception {
    byte[] expectedBytes = FILE_CONTENTS.getBytes(Charset.forName("UTF-8"));
    byte[] actualBytes = new byte[expectedBytes.length];
    int actualByteCount = fsInput.read(actualBytes, 0, actualBytes.length);

    assertThat(actualBytes, is(equalTo(expectedBytes)));
    assertThat(actualByteCount, is(equalTo(expectedBytes.length)));
  }

  @Test
  public void testSeek() throws Exception {
    int seekPos = FILE_CONTENTS.length() / 2;
    byte[] fileContentBytes = FILE_CONTENTS.getBytes(Charset.forName("UTF-8"));
    byte expectedByte = fileContentBytes[seekPos];
    fsInput.seek(seekPos);
    byte[] readBytes = new byte[1];
    fsInput.read(readBytes, 0, 1);
    byte actualByte = readBytes[0];
    assertThat(actualByte, is(equalTo(expectedByte)));
  }

  @Test
  public void testTell() throws Exception {
    long expectedTellPos = FILE_CONTENTS.length() / 2;
    fsInput.seek(expectedTellPos);
    long actualTellPos = fsInput.tell();
    assertThat(actualTellPos, is(equalTo(expectedTellPos)));
  }

}
