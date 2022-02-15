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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Avro.Generic;
using Avro.IO;

namespace Avro.File
{
    /// <summary>
    /// Stores in a file a sequence of data conforming to a schema. The schema is stored in the file
    /// with the data. Each datum in a file is of the same schema. Data is written with a
    /// <see cref="DatumWriter{T}" />. Data is grouped into blocks. A synchronization marker is
    /// written between blocks, so that files may be split. Blocks may be compressed. Extensible
    /// metadata is stored at the end of the file. Files may be appended to.
    /// </summary>
    /// <typeparam name="T">Type of datum to write to the file.</typeparam>
    /// <seealso cref="IFileWriter&lt;T&gt;" />
    public class DataFileWriter<T> : IFileWriter<T>
    {
        private Schema _schema;
        private Codec _codec;
        private Stream _stream;
        private bool _leaveOpen;
        private MemoryStream _blockStream;
        private MemoryStream _compressedBlockStream;
        private Encoder _encoder, _blockEncoder;
        private readonly DatumWriter<T> _writer;

        private byte[] _syncData;
        private bool _isOpen;
        private bool _headerWritten;
        private int _blockCount;
        private int _syncInterval;
        private IDictionary<string, byte[]> _metaData;

        /// <summary>
        /// Open a new writer instance to write
        /// to a file path, using a Null codec
        /// </summary>
        /// <param name="writer">Datum writer to use.</param>
        /// <param name="path">Path to the file.</param>
        /// <returns>
        /// A new file writer.
        /// </returns>
        public static IFileWriter<T> OpenWriter(DatumWriter<T> writer, string path) => OpenWriter(writer, new FileStream(path, FileMode.Create), Codec.CreateCodec(Codec.Type.Null));

        /// <summary>
        /// Open a new writer instance to write
        /// to an output stream, using a Null codec
        /// </summary>
        /// <param name="writer">Datum writer to use.</param>
        /// <param name="outStream">Stream to write to.</param>
        /// <returns>
        /// A new file writer.
        /// </returns>
        public static IFileWriter<T> OpenWriter(DatumWriter<T> writer, Stream outStream) => OpenWriter(writer, outStream, Codec.CreateCodec(Codec.Type.Null));

        /// <summary>
        /// Open a new writer instance to write
        /// to an output stream, using a Null codec
        /// </summary>
        /// <param name="writer">Datum writer to use.</param>
        /// <param name="outStream">Stream to write to.</param>
        /// <param name="leaveOpen">Leave the stream open after disposing the object</param>
        /// <returns>
        /// A new file writer.
        /// </returns>
        public static IFileWriter<T> OpenWriter(DatumWriter<T> writer, Stream outStream, bool leaveOpen) => OpenWriter(writer, outStream, Codec.CreateCodec(Codec.Type.Null), leaveOpen);

        /// <summary>
        /// Open a new writer instance to write
        /// to a file path with a specified codec
        /// </summary>
        /// <param name="writer">Datum writer to use.</param>
        /// <param name="path">Path to the file.</param>
        /// <param name="codec">Codec to use when writing.</param>
        /// <returns>
        /// A new file writer.
        /// </returns>
        public static IFileWriter<T> OpenWriter(DatumWriter<T> writer, string path, Codec codec) => OpenWriter(writer, new FileStream(path, FileMode.Create), codec);

        /// <summary>
        /// Open a new writer instance to write
        /// to an output stream with a specified codec
        /// </summary>
        /// <param name="writer">Datum writer to use.</param>
        /// <param name="outStream">Stream to write to.</param>
        /// <param name="codec">Codec to use when writing.</param>
        /// <returns>
        /// A new file writer.
        /// </returns>
        public static IFileWriter<T> OpenWriter(DatumWriter<T> writer, Stream outStream, Codec codec) => new DataFileWriter<T>(writer).Create(writer.Schema, outStream, codec, false);

        /// <summary>
        /// Open a new writer instance to write
        /// to an output stream with a specified codec
        /// </summary>
        /// <param name="writer">Datum writer to use.</param>
        /// <param name="outStream">Stream to write to.</param>
        /// <param name="codec">Codec to use when writing.</param>
        /// <param name="leaveOpen">Leave the stream open after disposing the object</param>
        /// <returns>
        /// A new file writer.
        /// </returns>
        public static IFileWriter<T> OpenWriter(DatumWriter<T> writer, Stream outStream, Codec codec, bool leaveOpen) => new DataFileWriter<T>(writer).Create(writer.Schema, outStream, codec, leaveOpen);

        /// <summary>
        /// Open a new writer instance to append to a file path.
        /// </summary>
        /// <param name="writer">Datum writer to use.</param>
        /// <param name="path">Path to the file.</param>
        /// <returns>
        /// A new file writer.
        /// </returns>
        public static IFileWriter<T> OpenAppendWriter(DatumWriter<T> writer, string path) => new DataFileWriter<T>(writer).AppendTo(path);

        /// <summary>
        /// Open a new writer instance to append to an output stream.
        /// Both in and out streams must point to the same file.
        /// </summary>
        /// <param name="writer">Datum writer to use.</param>
        /// <param name="inStream">reading the existing file.</param>
        /// <param name="outStream">stream to write to, positioned at the end of the existing file.</param>
        /// <returns>
        /// A new file writer.
        /// </returns>
        /// <exception cref="AvroRuntimeException">
        /// {nameof(inStream)} must have Read access
        /// or
        /// {nameof(outStream)} must have Write access
        /// </exception>
        public static IFileWriter<T> OpenAppendWriter(DatumWriter<T> writer, Stream inStream, Stream outStream) =>
            !inStream.CanRead ?
                throw new AvroRuntimeException($"{nameof(inStream)} must have Read access") :
                !outStream.CanWrite ?
                throw new AvroRuntimeException($"{nameof(outStream)} must have Write access") :
                new DataFileWriter<T>(writer).AppendTo(inStream, outStream);

        /// <summary>
        /// Initializes a new instance of the <see cref="DataFileWriter{T}"/> class.
        /// </summary>
        /// <param name="writer">The writer.</param>
        private DataFileWriter(DatumWriter<T> writer)
        {
            _writer = writer;
            _syncInterval = DataFileConstants.DefaultSyncInterval;
        }

        /// <inheritdoc/>
        public bool IsReservedMeta(string key) => key.StartsWith(DataFileConstants.MetaDataReserved, StringComparison.Ordinal);

        /// <summary>
        /// Set metadata pair.
        /// </summary>
        /// <param name="key">Metadata key.</param>
        /// <param name="value">Metadata value.</param>
        /// <exception cref="AvroRuntimeException">Cannot set reserved meta key: {key}</exception>
        public void SetMeta(string key, byte[] value)
        {
            if (IsReservedMeta(key))
            {
                throw new AvroRuntimeException($"Cannot set reserved meta key: {key}");
            }

            _metaData.Add(key, value);
        }

        /// <summary>
        /// Set metadata pair (long value).
        /// </summary>
        /// <param name="key">Metadata key.</param>
        /// <param name="value">Metadata value.</param>
        /// <exception cref="AvroRuntimeException"></exception>
        public void SetMeta(string key, long value)
        {
            try
            {
                SetMeta(key, GetByteValue(value.ToString(CultureInfo.InvariantCulture)));
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException(e.Message, e);
            }
        }

        /// <summary>
        /// Set metadata pair (string value).
        /// </summary>
        /// <param name="key">Metadata key.</param>
        /// <param name="value">Metadata value.</param>
        /// <exception cref="AvroRuntimeException"></exception>
        public void SetMeta(string key, string value)
        {
            try
            {
                SetMeta(key, GetByteValue(value));
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException(e.Message, e);
            }
        }

        /// <summary>
        /// Set the synchronization interval for this file or stream, in bytes. Valid values range
        /// from 32 to 2^30. Suggested values are between 2K and 2M.
        /// </summary>
        /// <param name="syncInterval">Approximate number of uncompressed bytes to write in each block.</param>
        /// <exception cref="AvroRuntimeException">Invalid sync interval value: {syncInterval}</exception>
        public void SetSyncInterval(int syncInterval)
        {
            if (syncInterval < 32 || syncInterval > (1 << 30))
            {
                throw new AvroRuntimeException($"Invalid sync interval value: {syncInterval}");
            }

            _syncInterval = syncInterval;
        }

        /// <summary>
        /// Append datum to a file or stream.
        /// </summary>
        /// <param name="datum">Datum to append.</param>
        /// <exception cref="AvroRuntimeException">Error appending datum to writer</exception>
        public void Append(T datum)
        {
            AssertOpen();
            EnsureHeader();

            long usedBuffer = _blockStream.Position;

            try
            {
                _writer.Write(datum, _blockEncoder);
            }
            catch (Exception e)
            {
                _blockStream.Position = usedBuffer;
                throw new AvroRuntimeException("Error appending datum to writer", e);
            }

            _blockCount++;
            WriteIfBlockFull();
        }

        /// <summary>
        /// Appends to file.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns>a file writer</returns>
        private IFileWriter<T> AppendTo(string path)
        {
            using (FileStream inStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                FileStream outStream = new FileStream(path, FileMode.Append);
                return AppendTo(inStream, outStream);
            }

            // outStream does not need to be closed here. It will be closed by invoking Close()
            // of this writer.
        }

        /// <summary>
        /// Appends to stream.
        /// </summary>
        /// <param name="inStream">The in stream.</param>
        /// <param name="outStream">The out stream.</param>
        /// <returns>The file writer</returns>
        private IFileWriter<T> AppendTo(Stream inStream, Stream outStream)
        {
            using (var dataFileReader = DataFileReader<T>.OpenReader(inStream))
            {
                var header = dataFileReader.GetHeader();
                _schema = header.Schema;
                _syncData = header.SyncData;
                _metaData = header.MetaData;
            }

            if (_metaData.TryGetValue(DataFileConstants.MetaDataCodec, out byte[] codecBytes))
            {
                string codec = System.Text.Encoding.UTF8.GetString(codecBytes);
                _codec = Codec.CreateCodecFromString(codec);
            }
            else
            {
                _codec = Codec.CreateCodec(Codec.Type.Null);
            }

            _headerWritten = true;
            _stream = outStream;
            _stream.Seek(0, SeekOrigin.End);

            Init();

            return this;
        }

        /// <summary>
        /// Ensures the header.
        /// </summary>
        private void EnsureHeader()
        {
            if (!_headerWritten)
            {
                WriteHeader();
                _headerWritten = true;
            }
        }

        /// <inheritdoc/>
        public void Flush()
        {
            EnsureHeader();
            SyncInternal();
        }

        /// <inheritdoc/>
        public long Sync()
        {
            SyncInternal();
            return _stream.Position;
        }

        /// <summary>
        /// Synchronizes the internal.
        /// </summary>
        private void SyncInternal()
        {
            AssertOpen();
            WriteBlock();
        }

        /// <inheritdoc/>
        public void Close()
        {
            EnsureHeader();
            Flush();
            _stream.Flush();
            if (!_leaveOpen)
            {
                _stream.Close();
            }

            _blockStream.Dispose();
            _compressedBlockStream.Dispose();
            _isOpen = false;
        }

        /// <summary>
        /// Writes the header.
        /// </summary>
        private void WriteHeader()
        {
            _encoder.WriteFixed(DataFileConstants.Magic);
            WriteMetaData();
            WriteSyncData();
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        private void Init()
        {
            _blockCount = 0;
            _encoder = new BinaryEncoder(_stream);
            _blockStream = new MemoryStream();
            _blockEncoder = new BinaryEncoder(_blockStream);
            _compressedBlockStream = new MemoryStream();

            if (_codec == null)
            {
                _codec = Codec.CreateCodec(Codec.Type.Null);
            }

            _isOpen = true;
        }

        /// <summary>
        /// Asserts the open.
        /// </summary>
        /// <exception cref="AvroRuntimeException">Cannot complete operation: avro file/stream not open</exception>
        private void AssertOpen()
        {
            if (!_isOpen)
            {
                throw new AvroRuntimeException("Cannot complete operation: avro file/stream not open");
            }
        }

        private IFileWriter<T> Create(Schema schema, Stream outStream, Codec codec, bool leaveOpen)
        {
            _codec = codec;
            _stream = outStream;
            _metaData = new Dictionary<string, byte[]>();
            _schema = schema;
            _leaveOpen = leaveOpen;

            Init();

            return this;
        }

        /// <summary>
        /// Writes the meta data.
        /// </summary>
        private void WriteMetaData()
        {
            // Add sync, code & schema to metadata
            GenerateSyncData();

            //SetMetaInternal(DataFileConstants.MetaDataSync, _syncData); - Avro 1.5.4 C
            SetMetaInternal(DataFileConstants.MetaDataCodec, GetByteValue(_codec.GetName()));
            SetMetaInternal(DataFileConstants.MetaDataSchema, GetByteValue(_schema.ToString()));

            // write metadata
            int size = _metaData.Count;
            _encoder.WriteInt(size);

            foreach (KeyValuePair<string, byte[]> metaPair in _metaData)
            {
                _encoder.WriteString(metaPair.Key);
                _encoder.WriteBytes(metaPair.Value);
            }

            _encoder.WriteMapEnd();
        }

        /// <summary>
        /// Writes if block full.
        /// </summary>
        private void WriteIfBlockFull()
        {
            if (BufferInUse() >= _syncInterval)
            {
                WriteBlock();
            }
        }

        /// <summary>
        /// Buffers the in use.
        /// </summary>
        /// <returns>
        /// Position of block stream
        /// </returns>
        private long BufferInUse() => _blockStream.Position;

        /// <summary>
        /// Writes the block.
        /// </summary>
        private void WriteBlock()
        {
            if (_blockCount > 0)
            {
                // write count
                _encoder.WriteLong(_blockCount);

                // write data
                _codec.Compress(_blockStream, _compressedBlockStream);
                _encoder.WriteBytes(_compressedBlockStream.GetBuffer(), 0, (int)_compressedBlockStream.Length);

                // write sync marker
                _encoder.WriteFixed(_syncData);

                // reset / re-init block
                _blockCount = 0;
                _blockStream.SetLength(0);
            }
        }

        /// <summary>
        /// Writes the synchronize data.
        /// </summary>
        private void WriteSyncData() => _encoder.WriteFixed(_syncData);

        /// <summary>
        /// Generates the synchronize data.
        /// </summary>
        private void GenerateSyncData()
        {
            _syncData = new byte[16];

            Random random = new Random();
            random.NextBytes(_syncData);
        }

        /// <summary>
        /// Sets the meta internal.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        private void SetMetaInternal(string key, byte[] value) => _metaData.Add(key, value);

        /// <summary>
        /// Gets the byte value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>byte array of string value</returns>
        private byte[] GetByteValue(string value) => System.Text.Encoding.UTF8.GetBytes(value);

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases resources associated with this <see cref="DataFileWriter{T}"/>.
        /// </summary>
        /// <param name="disposing">
        /// True if called from <see cref="Dispose()"/>; false otherwise.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            Close();
            if (disposing && !_leaveOpen)
            {
                _stream.Dispose();
            }
        }
    }
}
