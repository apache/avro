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
package org.apache.avro.io;

import java.io.InputStream;

/**
 * A factory for creating and configuring {@link Decoder}s.
 * <p/>
 * Factories are thread-safe, and are generally cached by applications for
 * performance reasons. Multiple instances are only required if multiple
 * concurrent configurations are needed.
 * 
 * @see Decoder
 */

public class DecoderFactory {
  private static final DecoderFactory DEFAULT_FACTORY = new DefaultDecoderFactory();
  static final int DEFAULT_BUFFER_SIZE = 32 * 1000;

  int binaryDecoderBufferSize = DEFAULT_BUFFER_SIZE;
  boolean preferDirect = false;

  /** Constructor for factory instances */
  public DecoderFactory() {
    super();
  }

  /**
   * Returns an immutable static DecoderFactory configured with default settings
   * All mutating methods throw IllegalArgumentExceptions. All creator methods
   * create objects with default settings.
   */
  public static DecoderFactory defaultFactory() {
    return DEFAULT_FACTORY;
  }

  /**
   * Configures this factory to use the specified buffer size when creating
   * Decoder instances that buffer their input. The default buffer size is
   * 32000 bytes.
   * 
   * @param size The preferred buffer size. Valid values are in the range [32,
   *          16*1024*1024]. Values outide this range are rounded to the nearest
   *          value in the range. Values less than 512 or greater than 1024*1024
   *          are not recommended.
   * @return This factory, to enable method chaining:
   * <pre>
   * DecoderFactory myFactory = new DecoderFactory().useBinaryDecoderBufferSize(4096);
   * </pre>
   */
  public DecoderFactory configureDecoderBufferSize(int size) {
    if (size < 32)
      size = 32;
    if (size > 16 * 1024 * 1024)
      size = 16 * 1024 * 1024;
    this.binaryDecoderBufferSize = size;
    return this;
  }
  
  /**
   * Returns this factory's configured preferred buffer size.  Used when creating
   * Decoder instances that buffer. See {@link #configureDecoderBufferSize}
   * @return The preferred buffer size, in bytes.
   */
  public int getConfiguredBufferSize() {
    return this.binaryDecoderBufferSize;
  }
  
  /**
   * Configures this factory to create "direct" BinaryDecoder instances when applicable.
   * <p/>
   * The default is false, since buffering or 'read-ahead' decoders can be 
   * twice as fast.  In most cases a normal BinaryDecoder is sufficient in combination with
   * {@link BinaryDecoder#inputStream()} which provides a buffer-aware view on
   * the data.
   * <p/>
   * A "direct" BinaryDecoder does not read ahead from an InputStream or other data source
   * that cannot be rewound.  From the perspective of a client, a "direct" decoder
   * must never read beyond the minimum necessary bytes to service a {@link BinaryDecoder}
   * API read request.  
   * <p/>
   * In the case that the performance of a normal BinaryDecoder does not outweigh the
   * inconvenience of its buffering semantics, a "direct" decoder can be
   * used.
   * <p/>
   * Generally, this distinction only applies to BinaryDecoder that read from an InputStream.
   * @param useDirect If true, this factory will generate "direct" BinaryDecoder
   * implementations when applicable. If false (the default) the faster buffering
   * implementations will be generated.
   * @return This factory, to enable method chaining:
   * <pre>
   * DecoderFactory myFactory = new DecoderFactory().configureDirectDecoder(true);
   * </pre>
   */
  public DecoderFactory configureDirectDecoder(boolean useDirect) {
    this.preferDirect = useDirect;
    return this;
  }

  /**
   * Creates or reinitializes a {@link BinaryDecoder} with the input stream
   * provided as the source of data. If <i>reuse</i> is provided, it will be
   * reinitialized to the given input stream.
   * <p/>
   * If this factory is configured to create "direct" BinaryDecoder instances,
   * this will return a non-buffering variant. Otherwise, this instance will
   * buffer the number of bytes configured by this factory, reading up to that
   * many bytes from the InputStream ahead of the minimum required for Decoder
   * API requests. {@link BinaryDecoder#inputStream()} provides a view on the data
   * that is buffer-aware, for users that need to access possibly buffered data
   * outside of the Decoder API.
   * 
   * @param in The InputStream to initialize to
   * @param reuse The BinaryDecoder to <i>attempt<i/> to reuse given the factory
   *          configuration. A specific BinaryDecoder implementation may not be
   *          compatible with reuse. For example, a BinaryDecoder created as
   *          'direct' can not be reinitialized to function in a non-'direct'
   *          mode. If <i>reuse<i/> is null a new instance is always created.
   * @return A BinaryDecoder that uses <i>in</i> as its source of data. If
   *         <i>reuse</i> is null, this will be a new instance. If <i>reuse</i>
   *         is not null, then it may be reinitialized if compatible, otherwise
   *         a new instance will be returned.
   *         <p/>
   *         example:
   * 
   *         <pre>
   * DecoderFactory factory = new DecoderFactory();
   * Decoder d = factory.createBinaryDecoder(input, null); // a new BinaryDecoder
   * d = createBinaryDecoder(input2, d); // reinitializes d to read from input2
   * factory.configureDirectDecoder(true);
   * Decoder d2 = factory.createBinaryDecoder(input3, d); // a new BinaryDecoder
   * </pre>
   * 
   *         <i>d2</i> above is not a reused instance of <i>d</d> because the
   *         latter is not 'direct' and can't be reused to create a 'direct'
   *         instance. Users must be careful to use the BinaryDecoder returned
   *         from the factory and not assume that the factory passed in the
   *         <i>reuse</i> argument
   */
  public BinaryDecoder createBinaryDecoder(InputStream in, BinaryDecoder reuse) {
    if (null == reuse) {
      if (preferDirect) {
        return new DirectBinaryDecoder(in);
      } else {
        return new BinaryDecoder(binaryDecoderBufferSize, in);
      }
    } else {
      if (!preferDirect) {
        if(reuse.getClass() == BinaryDecoder.class) {
          reuse.init(binaryDecoderBufferSize, in);
          return reuse;
        } else {
          return new BinaryDecoder(binaryDecoderBufferSize, in);
        }
      } else {
        if (reuse.getClass() == DirectBinaryDecoder.class) {
          ((DirectBinaryDecoder)reuse).init(in);
          return reuse;
        } else {
          return new DirectBinaryDecoder(in);
        }
      }
    }
  }

  /**
   * Creates or reinitializes a {@link BinaryDecoder} with the byte array
   * provided as the source of data. If <i>reuse</i> is provided, it will
   * attempt to reinitiailize <i>reuse</i> to the new byte array. This instance
   * will use the provided byte array as its buffer.
   * {@link BinaryDecoder#inputStream()} provides a view on the data that is
   * buffer-aware and can provide a view of the data not yet read by Decoder API
   * methods.
   * 
   * @param bytes The byte array to initialize to
   * @param offset The offset to start reading from
   * @param length The maximum number of bytes to read from the byte array
   * @param reuse The BinaryDecoder to attempt to reinitialize. if null a new
   *          BinaryDecoder is created.
   * @return A BinaryDecoder that uses <i>bytes</i> as its source of data. If
   *         <i>reuse</i> is null, this will be a new instance. <i>reuse</i> may
   *         be reinitialized if appropriate, otherwise a new instance is
   *         returned. Clients must not assume that <i>reuse</i> is
   *         reinitialized and returned.
   */
  public BinaryDecoder createBinaryDecoder(byte[] bytes, int offset,
      int length, BinaryDecoder reuse) {
    if (null != reuse && reuse.getClass() == BinaryDecoder.class) {
      reuse.init(bytes, offset, length);
      return reuse;
    } else {
      return new BinaryDecoder(bytes, offset, length);
    }
  }

  /**
   * This method is shorthand for
   * <pre>
   * createBinaryDecoder(bytes, 0, bytes.length, reuse);
   * </pre> {@link #createBinaryDecoder(byte[], int, int, BinaryDecoder)}
   */
  public BinaryDecoder createBinaryDecoder(byte[] bytes, BinaryDecoder reuse) {
    return createBinaryDecoder(bytes, 0, bytes.length, reuse);
  }

  private static class DefaultDecoderFactory extends DecoderFactory {
    @Override
    public DecoderFactory configureDecoderBufferSize(int bufferSize) {
      throw new IllegalArgumentException("This Factory instance is Immutable");
    }
    @Override
    public DecoderFactory configureDirectDecoder(boolean arg0) {
      throw new IllegalArgumentException("This Factory instance is Immutable");
    }
  }
}
