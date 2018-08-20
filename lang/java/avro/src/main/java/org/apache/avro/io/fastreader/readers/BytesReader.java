/*
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
package org.apache.avro.io.fastreader.readers;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.io.Decoder;

public class BytesReader implements FieldReader<ByteBuffer> {

  private static final BytesReader INSTANCE = new BytesReader();

  public static BytesReader get() {
    return INSTANCE;
  }

  private BytesReader() {}

  @Override
  public boolean canReuse() {
    return true;
  }

  @Override
  public ByteBuffer read(ByteBuffer reuse, Decoder decoder) throws IOException {
    return decoder.readBytes(reuse);
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    decoder.skipBytes();
  }
}
