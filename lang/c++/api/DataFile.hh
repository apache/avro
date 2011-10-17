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

/**
 * The sync value.
 */
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

public:
    /**
     * Returns the current encoder for this writer.
     */
    Encoder& encoder() const { return *encoderPtr_; }
    
    /**
     * Returns true if the buffer has sufficient data for a sync to be
     * inserted.
     */
    void syncIfNeeded();

    /**
     * Increments the object count.
     */
    void incr() {
        ++objectCount_;
    }
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
class DataFileWriter : boost::noncopyable {
    std::auto_ptr<DataFileWriterBase> base_;
public:
    /**
     * Constructs a new data file.
     */
    DataFileWriter(const char* filename, const ValidSchema& schema,
        size_t syncInterval = 16 * 1024) :
        base_(new DataFileWriterBase(filename, schema, syncInterval)) { }

    /**
     * Writes the given piece of data into the file.
     */
    void write(const T& datum) {
        base_->syncIfNeeded();
        avro::encode(base_->encoder(), datum);
        base_->incr();
    }

    /**
     * Closes the current file. Once closed this datafile object cannot be
     * used for writing any more.
     */
    void close() { base_->close(); }

    /**
     * Returns the schema for this data file.
     */
    const ValidSchema& schema() const { return base_->schema(); }

    /**
     * Flushes any unwritten data into the file.
     */
    void flush() { base_->flush(); }
};

/**
 * The type independent portion of rader.
 */
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

    bool readDataBlock();
public:
    /**
     * Returns the current decoder for this reader.
     */
    Decoder& decoder() { return *dataDecoder_; }

    /**
     * Returns true if and only if there is more to read.
     */
    bool hasMore();

    /**
     * Decrements the number of objects yet to read.
     */
    void decr() { --objectCount_; }

    /**
     * Constructs the reader for the given file and the reader is
     * expected to use the schema that is used with data.
     * This function should be called exactly once after constructing
     * the DataFileReaderBase object.
     */
    DataFileReaderBase(const char* filename);

    /**
     * Initializes the reader so that the reader and writer schemas
     * are the same.
     */
    void init();

    /**
     * Initializes the reader to read objects according to the given
     * schema. This gives an opportinity for the reader to see the schema
     * in the data file before deciding the right schema to use for reading.
     * This must be called exactly once after constructing the
     * DataFileReaderBase object.
     */
    void init(const ValidSchema& readerSchema);

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

/**
 * Reads the contents of data file one after another.
 */
template <typename T>
class DataFileReader : boost::noncopyable {
    std::auto_ptr<DataFileReaderBase> base_;
public:
    /**
     * Constructs the reader for the given file and the reader is
     * expected to use the given schema.
     */
    DataFileReader(const char* filename, const ValidSchema& readerSchema) :
        base_(new DataFileReaderBase(filename)) {
        base_->init(readerSchema);
    }

    /**
     * Constructs the reader for the given file and the reader is
     * expected to use the schema that is used with data.
     */
    DataFileReader(const char* filename) :
        base_(new DataFileReaderBase(filename)) {
        base_->init();
    }


    /**
     * Constructs a reader using the reader base. This form of constructor
     * allows the user to examine the schema of a given file and then
     * decide to use the right type of data to be desrialize. Without this
     * the user must know the type of data for the template _before_
     * he knows the schema within the file.
     * The schema present in the data file will be used for reading
     * from this reader.
     */
    DataFileReader(std::auto_ptr<DataFileReaderBase> base) : base_(base) {
        base_->init();
    }

    /**
     * Constructs a reader using the reader base. This form of constructor
     * allows the user to examine the schema of a given file and then
     * decide to use the right type of data to be desrialize. Without this
     * the user must know the type of data for the template _before_
     * he knows the schema within the file.
     * The argument readerSchema will be used for reading
     * from this reader.
     */
    DataFileReader(std::auto_ptr<DataFileReaderBase> base,
        const ValidSchema& readerSchema) : base_(base) {
        base_->init(readerSchema);
    }

    /**
     * Reads the next entry from the data file.
     * \return true if an object has been successfully read into \p datum and
     * false if there are no more entries in the file.
     */
    bool read(T& datum) {
        if (base_->hasMore()) {
            base_->decr();
            avro::decode(base_->decoder(), datum);
            return true;
        }
        return false;
    }

    /**
     * Returns the schema for this object.
     */
    const ValidSchema& readerSchema() { return base_->readerSchema(); }

    /**
     * Returns the schema stored with the data file.
     */
    const ValidSchema& dataSchema() { return base_->dataSchema(); }

    /**
     * Closes the reader. No further operation is possible on this reader.
     */
    void close() { return base_->close(); }
};

}   // namespace avro
#endif


