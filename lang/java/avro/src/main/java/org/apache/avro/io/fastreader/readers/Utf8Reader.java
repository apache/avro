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
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Utf8Reader implements FieldReader<Utf8> {

  private static final Utf8Reader INSTANCE = new Utf8Reader();

  public static Utf8Reader get() {
    return INSTANCE;
  }

  private Utf8Reader() {}

  @Override
  public boolean canReuse() {
    return true;
  }

  @Override
  public Utf8 read(Utf8 reuse, Decoder decoder) throws IOException {
    return decoder.readString(reuse);
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    decoder.skipString();
  }
}
