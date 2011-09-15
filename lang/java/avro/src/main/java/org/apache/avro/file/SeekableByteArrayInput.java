/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/** A {@link SeekableInput} backed data in a byte array. */
public class SeekableByteArrayInput implements SeekableInput {
	private final ByteArrayInputStream stream;
	private final long length;
	private long pos = 0;

	public SeekableByteArrayInput(byte[] data) {
		this.stream = new ByteArrayInputStream(data);
		this.length = data.length;
	}

	public long length() throws IOException {
		return this.length;
	}

	public void seek(long p) throws IOException {
		if (p < this.pos)
			throw new IOException("Cannot seek to a previously read part of the input.");
		this.stream.skip(p - this.pos);
		this.pos = p;
	}

	public long tell() throws IOException {
		return this.pos;
	}

	public int read(byte[] b, int off, int len) throws IOException {
		int bytesRead = this.stream.read(b, off, len);
		if (bytesRead > -1) {
			pos += bytesRead;
		}
		return bytesRead;
	}

	public void close() throws IOException {
		this.stream.close();
	}
}
