/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestFsInput {
  private static File file;
  private static final String FILE_CONTENTS = "abcdefghijklmnopqrstuvwxyz";
  public static final int LENGTH = FILE_CONTENTS.length();
  private Configuration conf;
  private FsInput fsInput;

  @TempDir
  public File DIR;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    file = new File(DIR, "file.txt");

    try (
        PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
      out.print(FILE_CONTENTS);
    }
    fsInput = new FsInput(new Path(file.getPath()), conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (fsInput != null) {
      fsInput.close();
    }
  }

  @Test
  void configurationConstructor() throws Exception {
    try (FsInput in = new FsInput(new Path(file.getPath()), conf)) {
      int expectedByteCount = 1;
      byte[] readBytes = new byte[expectedByteCount];
      int actualByteCount = in.read(readBytes, 0, expectedByteCount);
      assertThat(actualByteCount, is(equalTo(expectedByteCount)));
    }
  }

  @Test
  void fileSystemConstructor() throws Exception {
    Path path = new Path(file.getPath());
    FileSystem fs = path.getFileSystem(conf);
    try (FsInput in = new FsInput(path, fs)) {
      int expectedByteCount = 1;
      byte[] readBytes = new byte[expectedByteCount];
      int actualByteCount = in.read(readBytes, 0, expectedByteCount);
      assertThat(actualByteCount, is(equalTo(expectedByteCount)));
    }
  }

  @Test
  void length() throws IOException {
    assertEquals(fsInput.length(), FILE_CONTENTS.length());
  }

  @Test
  void read() throws Exception {
    byte[] expectedBytes = FILE_CONTENTS.getBytes(StandardCharsets.UTF_8);
    byte[] actualBytes = new byte[expectedBytes.length];
    int actualByteCount = fsInput.read(actualBytes, 0, actualBytes.length);

    assertThat(actualBytes, is(equalTo(expectedBytes)));
    assertThat(actualByteCount, is(equalTo(expectedBytes.length)));
  }

  @Test
  void seek() throws Exception {
    int seekPos = FILE_CONTENTS.length() / 2;
    byte[] fileContentBytes = FILE_CONTENTS.getBytes(StandardCharsets.UTF_8);
    byte expectedByte = fileContentBytes[seekPos];
    fsInput.seek(seekPos);
    byte[] readBytes = new byte[1];
    fsInput.read(readBytes, 0, 1);
    byte actualByte = readBytes[0];
    assertThat(actualByte, is(equalTo(expectedByte)));
  }

  @Test
  void tell() throws Exception {
    long expectedTellPos = FILE_CONTENTS.length() / 2;
    fsInput.seek(expectedTellPos);
    long actualTellPos = fsInput.tell();
    assertThat(actualTellPos, is(equalTo(expectedTellPos)));
  }

  /**
   * How does a negative seek manifest itself? It's expected to fail on the seek()
   * call and not move the file position. Most streams raise EOFExceotion; some
   * raise IllegalArgumentException instead.
   */
  @Test
  void seekNegative() throws Exception {
    fsInput.seek(1);
    assertThrows(Exception.class, () -> fsInput.seek(-1));
    assertThat("file position after a negative seek", fsInput.tell(), is(equalTo(1L)));
  }

  /**
   * Seek past the EOF then read.
   */
  @Test
  void seekPastEOF() {
    assertThrows(Exception.class, () -> fsInput.seek(LENGTH + 2));
  }

  /**
   * Read to the exact EOF then do a read(), which is required to return -1 to
   * indicate the EOF has been reached.
   */
  @Test
  void readAtEOF() throws Exception {
    fsInput.seek(LENGTH);
    final int l = 8;
    byte[] readBytes = new byte[l];
    assertThat("bytes read from beyond EOF", fsInput.read(readBytes, 0, l), is(equalTo(-1)));
  }

  /**
   * Read across the end of file. All data available up to the EOF is returned.
   */
  @Test
  void readAcrossEOF() throws Exception {
    fsInput.seek(LENGTH - 2);
    final int l = 8;
    byte[] readBytes = new byte[l];
    assertThat("bytes read from beyond EOF", fsInput.read(readBytes, 0, l), is(equalTo(2)));
    assertThat("bytes read from beyond EOF", fsInput.tell(), is(equalTo((long) LENGTH)));
  }

  /**
   * Delete the file before trying to open it.
   */
  @Test
  void openMissingFile() {
    file.delete();
    assertThrows(FileNotFoundException.class, () -> new FsInput(new Path(file.getPath()), conf).close());
  }

}
