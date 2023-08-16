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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.file;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

public class TestSeekableInputStream {
    @Test
    public void testSkip(@TempDir Path tempDir) throws IOException {
        File f = tempDir.resolve("testSkip.avro").toFile();
        try (FileWriter w = new FileWriter(f)) {
            w.write("someContent");
        }
        try(SeekableFileInput in = new SeekableFileInput(f);
            DataFileReader.SeekableInputStream stream = new DataFileReader.SeekableInputStream(in)
        ) {
            for (long i = 0; i < stream.length(); i++) {
                Assertions.assertEquals(stream.length() - i, stream.available());
                Assertions.assertEquals(1, stream.skip(1));
            }
        }
    }
}
