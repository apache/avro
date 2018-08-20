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
package org.apache.avro.io.fastreader.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingDecoder extends Decoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingDecoder.class);

  private final Decoder parentDecoder;

  public LoggingDecoder(Decoder parentDecoder) {
    this.parentDecoder = parentDecoder;
  }

  @Override
  public void readNull() throws IOException {
    LOGGER.info("readNull");
    parentDecoder.readNull();
  }

  @Override
  public boolean readBoolean() throws IOException {
    LOGGER.info("readBoolean");
    return parentDecoder.readBoolean();
  }

  @Override
  public int readInt() throws IOException {
    LOGGER.info("readInt");
    return parentDecoder.readInt();
  }

  @Override
  public long readLong() throws IOException {
    LOGGER.info("readLong");
    return parentDecoder.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    LOGGER.info("readFloat");
    return parentDecoder.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    LOGGER.info("readDouble");
    return parentDecoder.readDouble();
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    LOGGER.info("readString(UTF8)");
    return parentDecoder.readString(old);
  }

  @Override
  public String readString() throws IOException {
    LOGGER.info("readString");
    return parentDecoder.readString();
  }

  @Override
  public void skipString() throws IOException {
    LOGGER.info("skipString");
    parentDecoder.skipString();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    LOGGER.info("readBytes");
    return parentDecoder.readBytes(old);
  }

  @Override
  public void skipBytes() throws IOException {
    LOGGER.info("skipBytes");
    parentDecoder.skipBytes();
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    LOGGER.info("readFixed(length={})", length);
    parentDecoder.readFixed(bytes, start, length);
  }

  @Override
  public void skipFixed(int length) throws IOException {
    LOGGER.info("skipFixed(length={})", length);
    parentDecoder.skipFixed(length);
  }

  @Override
  public int readEnum() throws IOException {
    LOGGER.info("readEnum");
    return parentDecoder.readEnum();
  }

  @Override
  public long readArrayStart() throws IOException {
    LOGGER.info("readArrayStart");
    return parentDecoder.readArrayStart();
  }

  @Override
  public long arrayNext() throws IOException {
    LOGGER.info("arrayNext");
    return parentDecoder.arrayNext();
  }

  @Override
  public long skipArray() throws IOException {
    LOGGER.info("skipArray");
    return parentDecoder.skipArray();
  }

  @Override
  public long readMapStart() throws IOException {
    LOGGER.info("readMapStart");
    return parentDecoder.readMapStart();
  }

  @Override
  public long mapNext() throws IOException {
    LOGGER.info("mapNext");
    return parentDecoder.mapNext();
  }

  @Override
  public long skipMap() throws IOException {
    LOGGER.info("skipMap");
    return parentDecoder.skipMap();
  }

  @Override
  public int readIndex() throws IOException {
    LOGGER.info("readIndex");
    return parentDecoder.readIndex();
  }
}
