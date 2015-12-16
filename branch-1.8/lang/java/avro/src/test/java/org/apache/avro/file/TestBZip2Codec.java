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
package org.apache.avro.file;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestBZip2Codec {
  
  @Test
  public void testBZip2CompressionAndDecompression() throws IOException {
    Codec codec = CodecFactory.fromString("bzip2").createInstance();
    assertTrue(codec instanceof BZip2Codec);
    assertTrue(codec.getName().equals("bzip2"));
    
    //This is 3 times the byte buffer on the BZip2 decompress plus some extra
    final int inputByteSize = BZip2Codec.DEFAULT_BUFFER_SIZE * 3 + 42;
    
    byte[] inputByteArray = new byte[inputByteSize];
    
    //Generate something that will compress well
    for (int i = 0; i < inputByteSize; i++) {
      inputByteArray[i] = (byte)(65 + i % 10);
    }
    
    ByteBuffer inputByteBuffer = ByteBuffer.allocate(inputByteSize * 2);
    inputByteBuffer.put(inputByteArray);
    
    ByteBuffer compressedBuffer = codec.compress(inputByteBuffer);
    
    //Make sure something returned
    assertTrue(compressedBuffer.array().length > 0);
    //Make sure the compressed output is smaller then the original
    assertTrue(compressedBuffer.array().length < inputByteArray.length);
    
    ByteBuffer decompressedBuffer = codec.decompress(compressedBuffer);
    
    //The original array should be the same length as the decompressed array
    assertTrue(decompressedBuffer.array().length == inputByteArray.length);
    
    //Every byte in the outputByteArray should equal every byte in the input array 
    byte[] outputByteArray = decompressedBuffer.array();
    for (int i = 0; i < inputByteSize; i++) {
      inputByteArray[i] = outputByteArray[i];
    }
  }
}
