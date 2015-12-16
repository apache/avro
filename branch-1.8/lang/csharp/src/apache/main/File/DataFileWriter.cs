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
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Avro.IO;
using Avro.Generic;

namespace Avro.File
{
    public class DataFileWriter<T> : IFileWriter<T>
    {
        private Schema _schema;
        private Codec _codec;
        private Stream _stream;
        private MemoryStream _blockStream;
        private Encoder _encoder, _blockEncoder;
        private DatumWriter<T> _writer;

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
        /// <param name="writer"></param>
        /// <param name="path"></param>
        /// <returns></returns>
        public static IFileWriter<T> OpenWriter(DatumWriter<T> writer, string path)
        {
            return OpenWriter(writer, new FileStream(path, FileMode.Create), Codec.CreateCodec(Codec.Type.Null));
        }

        /// <summary>
        /// Open a new writer instance to write  
        /// to an output stream, using a Null codec
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="outStream"></param>
        /// <returns></returns>
        public static IFileWriter<T> OpenWriter(DatumWriter<T> writer, Stream outStream)
        {
            return OpenWriter(writer, outStream, Codec.CreateCodec(Codec.Type.Null));
        }

        /// <summary>
        /// Open a new writer instance to write  
        /// to a file path with a specified codec
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="path"></param>
        /// <param name="codec"></param>
        /// <returns></returns>
        public static IFileWriter<T> OpenWriter(DatumWriter<T> writer, string path, Codec codec)
        {
            return OpenWriter(writer, new FileStream(path, FileMode.Create), codec);
        }

        /// <summary>
        /// Open a new writer instance to write
        /// to an output stream with a specified codec
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="outStream"></param>
        /// <param name="codec"></param>
        /// <returns></returns>
        public static IFileWriter<T> OpenWriter(DatumWriter<T> writer, Stream outStream, Codec codec)
        {
            return new DataFileWriter<T>(writer).Create(writer.Schema, outStream, codec);
        }

        DataFileWriter(DatumWriter<T> writer)
        {
            _writer = writer;
            _syncInterval = DataFileConstants.DefaultSyncInterval;
        }

        public bool IsReservedMeta(string key)
        {
            return key.StartsWith(DataFileConstants.MetaDataReserved);
        }

        public void SetMeta(String key, byte[] value)
        {
            if (IsReservedMeta(key))
            {
                throw new AvroRuntimeException("Cannot set reserved meta key: " + key);
            }
            _metaData.Add(key, value);
        }

        public void SetMeta(String key, long value)
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

        public void SetMeta(String key, string value)
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

        public void SetSyncInterval(int syncInterval)
        {
            if (syncInterval < 32 || syncInterval > (1 << 30))
            {
                throw new AvroRuntimeException("Invalid sync interval value: " + syncInterval);
            }
            _syncInterval = syncInterval;
        }

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

        private void EnsureHeader()
        {
            if (!_headerWritten)
            {
                WriteHeader();
                _headerWritten = true;
            }
        }

        public void Flush()
        {
            EnsureHeader();
            Sync();
        }

        public long Sync()
        {
            AssertOpen();
            WriteBlock();
            return _stream.Position;
        }

        public void Close()
        {
            EnsureHeader();
            Flush();
            _stream.Flush();
            _stream.Close();
            _isOpen = false;
        }

        private void WriteHeader()
        {
            _encoder.WriteFixed(DataFileConstants.Magic);
            WriteMetaData();
            WriteSyncData();
        }

        private void Init()
        {
            _blockCount = 0;
            _encoder = new BinaryEncoder(_stream);
            _blockStream = new MemoryStream();
            _blockEncoder = new BinaryEncoder(_blockStream);

            if (_codec == null)
                _codec = Codec.CreateCodec(Codec.Type.Null);

            _isOpen = true;
        }

        private void AssertOpen()
        {
            if (!_isOpen) throw new AvroRuntimeException("Cannot complete operation: avro file/stream not open");
        }

        private IFileWriter<T> Create(Schema schema, Stream outStream, Codec codec)
        {
            _codec = codec;
            _stream = outStream;
            _metaData = new Dictionary<string, byte[]>();
            _schema = schema;

            Init();

            return this;
        }

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

            foreach (KeyValuePair<String, byte[]> metaPair in _metaData)
            {
                _encoder.WriteString(metaPair.Key);
                _encoder.WriteBytes(metaPair.Value);
            }
            _encoder.WriteMapEnd();
        }

        private void WriteIfBlockFull()
        {
            if (BufferInUse() >= _syncInterval)
                WriteBlock();
        }

        private long BufferInUse()
        {
            return _blockStream.Position;
        }

        private void WriteBlock() 
        { 
            if (_blockCount > 0) 
            {
                byte[] dataToWrite = _blockStream.ToArray();

                // write count 
                _encoder.WriteLong(_blockCount);

                // write data 
                _encoder.WriteBytes(_codec.Compress(dataToWrite));
                    
                // write sync marker 
                _encoder.WriteFixed(_syncData);
            
                // reset / re-init block
                _blockCount = 0;
                _blockStream = new MemoryStream();
                _blockEncoder = new BinaryEncoder(_blockStream);
            }
        }

        private void WriteSyncData()
        {
            _encoder.WriteFixed(_syncData);
        }

        private void GenerateSyncData()
        {
            _syncData = new byte[16];

            Random random = new Random();
            random.NextBytes(_syncData);
        }

        private void SetMetaInternal(string key, byte[] value)
        {
            _metaData.Add(key, value);
        }
  
        private byte[] GetByteValue(string value)
        {
            return System.Text.Encoding.UTF8.GetBytes(value);
        }

        public void Dispose()
        {
            Close();
        }
    }
}
