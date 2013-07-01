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
using System.Linq;
using System.IO;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;

namespace Avro.File
{
    public class DataFileReader<T> : IFileReader<T>
    {
        private DatumReader<T> _reader;
        private Decoder _decoder, _datumDecoder;
        private Header _header;
        private Codec _codec;
        private DataBlock _currentBlock;
        private long _blockRemaining;
        private long _blockSize;
        private bool _availableBlock;
        private byte[] _syncBuffer;
        private long _blockStart;
        private Stream _stream;
        private Schema _readerSchema;

        /// <summary>
        ///  Open a reader for a file using path
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static IFileReader<T> OpenReader(string path)
        {
            return OpenReader(new FileStream(path, FileMode.Open), null);
        }

        /// <summary>
        ///  Open a reader for a file using path and the reader's schema
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static IFileReader<T> OpenReader(string path, Schema readerSchema)
        {
            return OpenReader(new FileStream(path, FileMode.Open), readerSchema);
        }

        /// <summary>
        ///  Open a reader for a stream
        /// </summary>
        /// <param name="inStream"></param>
        /// <returns></returns>
        public static IFileReader<T> OpenReader(Stream inStream)
        {
            return OpenReader(inStream, null);
        }

        /// <summary>
        ///  Open a reader for a stream and using the reader's schema
        /// </summary>
        /// <param name="inStream"></param>
        /// <returns></returns>
        public static IFileReader<T> OpenReader(Stream inStream, Schema readerSchema)
        {
            if (!inStream.CanSeek)
                throw new AvroRuntimeException("Not a valid input stream - must be seekable!");

            if (inStream.Length < DataFileConstants.Magic.Length)
                throw new AvroRuntimeException("Not an Avro data file");

            // verify magic header
            byte[] magic = new byte[DataFileConstants.Magic.Length];
            inStream.Seek(0, SeekOrigin.Begin);
            for (int c = 0; c < magic.Length; c = inStream.Read(magic, c, magic.Length - c)) { }
            inStream.Seek(0, SeekOrigin.Begin);

            if (magic.SequenceEqual(DataFileConstants.Magic))   // current format
                return new DataFileReader<T>(inStream, readerSchema);         // (not supporting 1.2 or below, format) 

            throw new AvroRuntimeException("Not an Avro data file");
        }

        DataFileReader(Stream stream, Schema readerSchema)
        {
            _readerSchema = readerSchema;
            Init(stream);
            BlockFinished();
        }

        public Header GetHeader()
        {
            return _header;
        }

        public Schema GetSchema()
        {
            return _header.Schema;
        }

        public ICollection<string> GetMetaKeys()
        {
            return _header.MetaData.Keys;
        }

        public byte[] GetMeta(string key)
        {
            try
            {
                return _header.MetaData[key];
            }
            catch (KeyNotFoundException knfe)
            {
                return null; 
            }
        }

        public long GetMetaLong(string key)
        {
            return long.Parse(GetMetaString(key));
        }

        public string GetMetaString(string key)
        {
            byte[] value = GetMeta(key);
            if (value == null)
            {
                return null;
            }
            try
            {
                return System.Text.Encoding.UTF8.GetString(value);          
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException(string.Format("Error fetching meta data for key: {0}", key), e);
            }
        }

        public void Seek(long position)
        {
            _stream.Position = position;
            _decoder = new BinaryDecoder(_stream);
            _datumDecoder = null;
            _blockRemaining = 0;
            _blockStart = position;
        }

        public void Sync(long position)
        {
            Seek(position);
            // work around an issue where 1.5.4 C stored sync in metadata
            if ((position == 0) && (GetMeta(DataFileConstants.MetaDataSync) != null)) 
            {
                Init(_stream); // re-init to skip header
                return;
            }

            try
            {
                bool done = false;

                do // read until sync mark matched
                {
                    _decoder.ReadFixed(_syncBuffer);
                    if (Enumerable.SequenceEqual(_syncBuffer, _header.SyncData))
                        done = true;
                    else
                        _stream.Position = _stream.Position - (DataFileConstants.SyncSize - 1);
                } while (!done);
            }
            catch (Exception e) { } // could not find .. default to EOF

            _blockStart = _stream.Position;
        }

        public bool PastSync(long position)
        {
            return ((_blockStart >= position + DataFileConstants.SyncSize) || (_blockStart >= _stream.Length));
        }

        public long PreviousSync()
        {
            return _blockStart;
        }

        public long Tell()
        {
            return _stream.Position;
        }

        public IEnumerable<T> NextEntries
        {
            get
            {
                while (HasNext())
                {
                    yield return Next();
                }
            }
        }

        public bool HasNext()
        {
            try
            {
                if (_blockRemaining == 0)
                {
                    // TODO: Check that the (block) stream is not partially read
                    /*if (_datumDecoder != null) 
                    { }*/
                    if (HasNextBlock())
                    {
                        _currentBlock = NextRawBlock(_currentBlock);
                        _currentBlock.Data = _codec.Decompress(_currentBlock.Data);
                        _datumDecoder = new BinaryDecoder(_currentBlock.GetDataAsStream());
                    }
                }
                return _blockRemaining != 0;
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException(string.Format("Error fetching next object from block: {0}", e));
            }
        }

        public void Reset()
        {
            Init(_stream);
        }

        public void Dispose()
        {
            _stream.Close();
        }

        private void Init(Stream stream)
        {
            _stream = stream;
            _header = new Header();
            _decoder = new BinaryDecoder(stream);
            _syncBuffer = new byte[DataFileConstants.SyncSize];

            // read magic 
            byte[] firstBytes = new byte[DataFileConstants.Magic.Length];
            try
            {
                _decoder.ReadFixed(firstBytes);
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException("Not a valid data file!", e);
            }
            if (!firstBytes.SequenceEqual(DataFileConstants.Magic))
                throw new AvroRuntimeException("Not a valid data file!");

            // read meta data 
            long len = _decoder.ReadMapStart();
            if (len > 0)
            {
                do
                {
                    for (long i = 0; i < len; i++)
                    {
                        string key = _decoder.ReadString();
                        byte[] val = _decoder.ReadBytes();
                        _header.MetaData.Add(key, val);
                    }
                } while ((len = _decoder.ReadMapNext()) != 0);
            }

            // read in sync data 
            _decoder.ReadFixed(_header.SyncData);

            // parse schema and set codec 
            _header.Schema = Schema.Parse(GetMetaString(DataFileConstants.MetaDataSchema));
            _reader = GetReaderFromSchema();
            _codec = ResolveCodec();
        }

        private DatumReader<T> GetReaderFromSchema()
        {
            DatumReader<T> reader = null;
            Type type = typeof(T);

            if (typeof(ISpecificRecord).IsAssignableFrom(type))
            {
                reader = new SpecificReader<T>(_header.Schema, _readerSchema ?? _header.Schema);
            }
            else // generic
            {
                reader = new GenericReader<T>(_header.Schema, _readerSchema ?? _header.Schema);
            }
            return reader;
        }

        private Codec ResolveCodec()
        {
            return Codec.CreateCodecFromString(GetMetaString(DataFileConstants.MetaDataCodec));
        }

        public T Next()
        {
            return Next(default(T));
        }

        private T Next(T reuse)
        {
            try
            {
                if (!HasNext())
                    throw new AvroRuntimeException("No more datum objects remaining in block!");

                T result = _reader.Read(reuse, _datumDecoder);
                if (--_blockRemaining == 0)
                {
                    BlockFinished();
                }
                return result;
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException(string.Format("Error fetching next object from block: {0}", e));
            }
        }

        private void BlockFinished()
        {
            _blockStart = _stream.Position;
        }

        private DataBlock NextRawBlock(DataBlock reuse)
        {
            if (!HasNextBlock())
                throw new AvroRuntimeException("No data remaining in block!");

            if (reuse == null || reuse.Data.Length < _blockSize)
            {
                reuse = new DataBlock(_blockRemaining, _blockSize);
            }
            else
            {
                reuse.NumberOfEntries = _blockRemaining;
                reuse.BlockSize = _blockSize;
            }

            _decoder.ReadFixed(reuse.Data, 0, (int)reuse.BlockSize);
            _decoder.ReadFixed(_syncBuffer);

            if (!Enumerable.SequenceEqual(_syncBuffer, _header.SyncData))
                throw new AvroRuntimeException("Invalid sync!");

            _availableBlock = false;
            return reuse;
        }

        private bool DataLeft()
        {
            long currentPosition = _stream.Position;
            if (_stream.ReadByte() != -1)
                _stream.Position = currentPosition;
            else
                return false;

            return true;
        }

        private bool HasNextBlock()
        {
            try
            {
                // block currently being read 
                if (_availableBlock)
                    return true;

                // check to ensure still data to read 
                if (!DataLeft())
                    return false;

                _blockRemaining = _decoder.ReadLong();      // read block count
                _blockSize = _decoder.ReadLong();           // read block size
                if (_blockSize > System.Int32.MaxValue || _blockSize < 0)
                {
                    throw new AvroRuntimeException("Block size invalid or too large for this " +
                                                   "implementation: " + _blockSize);
                }
                _availableBlock = true;
                return true;
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException(string.Format("Error ascertaining if data has next block: {0}", e));
            }
        }
    }
}