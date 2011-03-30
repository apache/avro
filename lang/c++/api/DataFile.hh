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

#ifndef avro_DataFile_hh__
#define avro_DataFile_hh__

#include "Encoder.hh"
#include "buffer/Buffer.hh"
#include "ValidSchema.hh"
#include "Specific.hh"
#include "Stream.hh"

#include <map>
#include <string>
#include <vector>

#include "boost/array.hpp"
#include "boost/utility.hpp"

namespace avro {

typedef boost::array<uint8_t, 16> DataFileSync;

/**
 * Type-independent portion of DataFileWriter.
 *  At any given point in time, at most one file can be written using
 *  this object.
 */
class DataFileWriterBase : boost::noncopyable {
    const std::string filename_;
    const ValidSchema schema_;
    const EncoderPtr encoderPtr_;
    const size_t syncInterval_;

    std::auto_ptr<OutputStream> stream_;
    std::auto_ptr<OutputStream> buffer_;
    const DataFileSync sync_;
    int64_t objectCount_;

    typedef std::map<std::string, std::vector<uint8_t> > Metadata;

    Metadata metadata_;

    static std::auto_ptr<OutputStream> makeStream(const char* filename);
    static DataFileSync makeSync();

    void writeHeader();
    void setMetadata(const std::string& key, const std::string& value);

    /**
     * Generates a sync marker in the file.
     */
    void sync();

protected:
    Encoder& encoder() const { return *encoderPtr_; }
    
    void syncIfNeeded();

    void incr() {
        ++objectCount_;
    }
public:
    /**
     * Constructs a data file writer with the given sync interval and name.
     */
    DataFileWriterBase(const char* filename, const ValidSchema& schema,
        size_t syncInterval);

    ~DataFileWriterBase();
    /**
     * Closes the current file. Once closed this datafile object cannot be
     * used for writing any more.
     */
    void close();

    /**
     * Returns the schema for this data file.
     */
    const ValidSchema& schema() const { return schema_; }

    /**
     * Flushes any unwritten data into the file.
     */
    void flush();
};

/**
 *  An Avro datafile that can store objects of type T.
 */
template <typename T>
class DataFileWriter : public DataFileWriterBase {
public:
    /**
     * Constructs a new data file.
     */
    DataFileWriter(const char* filename, const ValidSchema& schema,
        size_t syncInterval = 16 * 1024) :
        DataFileWriterBase(filename, schema, syncInterval) { }

    /**
     * Writes the given piece of data into the file.
     */
    void write(const T& datum) {
        syncIfNeeded();
        avro::encode(encoder(), datum);
        incr();
    }
};

class DataFileReaderBase : boost::noncopyable {
    const std::string filename_;
    const std::auto_ptr<InputStream> stream_;
    const DecoderPtr decoder_;
    int64_t objectCount_;

    ValidSchema readerSchema_;
    ValidSchema dataSchema_;
    DecoderPtr dataDecoder_;
    std::auto_ptr<InputStream> dataStream_;
    typedef std::map<std::string, std::vector<uint8_t> > Metadata;

    Metadata metadata_;
    DataFileSync sync_;

    void readHeader();

protected:
    Decoder& decoder() { return *dataDecoder_; }

    /**
     * Returns true if and only if there is more to read.
     */
    bool hasMore();

    void decr() { --objectCount_; }
    bool readDataBlock();

public:
    /**
     * Constructs the reader for the given file and the reader is
     * expected to use the given schema.
     */
    DataFileReaderBase(const char* filename, const ValidSchema& readerSchema);

    /**
     * Constructs the reader for the given file and the reader is
     * expected to use the schema that is used with data.
     */
    DataFileReaderBase(const char* filename);

    /**
     * Returns the schema for this object.
     */
    const ValidSchema& readerSchema() { return readerSchema_; }

    /**
     * Returns the schema stored with the data file.
     */
    const ValidSchema& dataSchema() { return dataSchema_; }

    /**
     * Closes the reader. No further operation is possible on this reader.
     */
    void close();
};

template <typename T>
class DataFileReader : public DataFileReaderBase {
public:
    /**
     * Constructs the reader for the given file and the reader is
     * expected to use the given schema.
     */
    DataFileReader(const char* filename, const ValidSchema& readerSchema) :
        DataFileReaderBase(filename, readerSchema) { }

    /**
     * Constructs the reader for the given file and the reader is
     * expected to use the schema that is used with data.
     */
    DataFileReader(const char* filename) : DataFileReaderBase(filename) { }

    bool read(T& datum) {
        if (hasMore()) {
            decr();
            avro::decode(decoder(), datum);
            return true;
        }
        return false;
    }
};

}   // namespace avro
#endif


