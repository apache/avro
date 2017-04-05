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

#include "DataFile.hh"
#include "Compiler.hh"
#include "Exception.hh"

#include <sstream>

#include <boost/random/mersenne_twister.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/crc.hpp>  // for boost::crc_32_type

#ifdef SNAPPY_CODEC_AVAILABLE
#include <snappy.h>
#endif

namespace avro {
using std::auto_ptr;
using std::ostringstream;
using std::istringstream;
using std::vector;
using std::copy;
using std::string;

using boost::array;

namespace {
const string AVRO_SCHEMA_KEY("avro.schema");
const string AVRO_CODEC_KEY("avro.codec");
const string AVRO_NULL_CODEC("null");
const string AVRO_DEFLATE_CODEC("deflate");

#ifdef SNAPPY_CODEC_AVAILABLE
const string AVRO_SNAPPY_CODEC = "snappy";
#endif

const size_t minSyncInterval = 32;
const size_t maxSyncInterval = 1u << 30;
const size_t defaultSyncInterval = 64 * 1024;

boost::iostreams::zlib_params get_zlib_params() {
  boost::iostreams::zlib_params ret;
  ret.method = boost::iostreams::zlib::deflated;
  ret.noheader = true;
  return ret;
}
}


static string toString(const ValidSchema& schema)
{
    ostringstream oss;
    schema.toJson(oss);
    return oss.str();
}

DataFileWriterBase::DataFileWriterBase(const char* filename,
    const ValidSchema& schema, size_t syncInterval, Codec codec) :
    filename_(filename), schema_(schema), encoderPtr_(binaryEncoder()),
    syncInterval_(syncInterval),
    codec_(codec),
    stream_(fileOutputStream(filename)),
    buffer_(memoryOutputStream()),
    sync_(makeSync()), objectCount_(0)
{
    if (syncInterval < minSyncInterval || syncInterval > maxSyncInterval) {
        throw Exception(boost::format("Invalid sync interval: %1%. "
            "Should be between %2% and %3%") % syncInterval %
            minSyncInterval % maxSyncInterval);
    }
    setMetadata(AVRO_CODEC_KEY, AVRO_NULL_CODEC);

    if (codec_ == NULL_CODEC) {
      setMetadata(AVRO_CODEC_KEY, AVRO_NULL_CODEC);
    } else if (codec_ == DEFLATE_CODEC) {
      setMetadata(AVRO_CODEC_KEY, AVRO_DEFLATE_CODEC);
#ifdef SNAPPY_CODEC_AVAILABLE
    } else if (codec_ == SNAPPY_CODEC) {
      setMetadata(AVRO_CODEC_KEY, AVRO_SNAPPY_CODEC);
#endif
    } else {
      throw Exception(boost::format("Unknown codec: %1%") % codec);
    }
    setMetadata(AVRO_SCHEMA_KEY, toString(schema));

    writeHeader();
    encoderPtr_->init(*buffer_);
}

DataFileWriterBase::~DataFileWriterBase()
{
    if (stream_.get()) {
        close();
    }
}

void DataFileWriterBase::close()
{
    flush();
    stream_.reset();
}

void DataFileWriterBase::sync()
{
    encoderPtr_->flush();

    encoderPtr_->init(*stream_);
    avro::encode(*encoderPtr_, objectCount_);
    if (codec_ == NULL_CODEC) {
        int64_t byteCount = buffer_->byteCount();
        avro::encode(*encoderPtr_, byteCount);
        encoderPtr_->flush();
        std::auto_ptr<InputStream> in = memoryInputStream(*buffer_);
        copy(*in, *stream_);
    } else if (codec_ == DEFLATE_CODEC) {
        std::vector<char> buf;
        {
            boost::iostreams::filtering_ostream os;
            os.push(boost::iostreams::zlib_compressor(get_zlib_params()));
            os.push(boost::iostreams::back_inserter(buf));
            const uint8_t* data;
            size_t len;

            std::auto_ptr<InputStream> input = memoryInputStream(*buffer_);
            while (input->next(&data, &len)) {
                boost::iostreams::write(os, reinterpret_cast<const char*>(data), len);
            }
        } // make sure all is flushed
        std::auto_ptr<InputStream> in = memoryInputStream(
           reinterpret_cast<const uint8_t*>(&buf[0]), buf.size());
        int64_t byteCount = buf.size();
        avro::encode(*encoderPtr_, byteCount);
        encoderPtr_->flush();
        copy(*in, *stream_);
#ifdef SNAPPY_CODEC_AVAILABLE
    } else if (codec_ == SNAPPY_CODEC) {
        std::vector<char> temp;
        std::string compressed;
        boost::crc_32_type crc;
        {
            boost::iostreams::filtering_ostream os;
            os.push(boost::iostreams::back_inserter(temp));
            const uint8_t* data;
            size_t len;

            std::auto_ptr<InputStream> input = memoryInputStream(*buffer_);
            while (input->next(&data, &len)) {
                boost::iostreams::write(os, reinterpret_cast<const char*>(data),
                        len);
            }
        } // make sure all is flushed

        crc.process_bytes(reinterpret_cast<const char*>(&temp[0]), temp.size());
        // For Snappy, add the CRC32 checksum
        int32_t checksum = crc();

        // Now compress
        size_t compressed_size = snappy::Compress(
                reinterpret_cast<const char*>(&temp[0]), temp.size(),
                &compressed);
        temp.clear();
        {
            boost::iostreams::filtering_ostream os;
            os.push(boost::iostreams::back_inserter(temp));
            boost::iostreams::write(os, compressed.c_str(), compressed_size);
        }
        temp.push_back((checksum >> 24) & 0xFF);
        temp.push_back((checksum >> 16) & 0xFF);
        temp.push_back((checksum >> 8) & 0xFF);
        temp.push_back(checksum & 0xFF);
        std::auto_ptr<InputStream> in = memoryInputStream(
                reinterpret_cast<const uint8_t*>(&temp[0]), temp.size());
        int64_t byteCount = temp.size();
        avro::encode(*encoderPtr_, byteCount);
        encoderPtr_->flush();
        copy(*in, *stream_);
#endif
    }

    encoderPtr_->init(*stream_);
    avro::encode(*encoderPtr_, sync_);
    encoderPtr_->flush();


    buffer_ = memoryOutputStream();
    encoderPtr_->init(*buffer_);
    objectCount_ = 0;
}

void DataFileWriterBase::syncIfNeeded()
{
    encoderPtr_->flush();
    if (buffer_->byteCount() >= syncInterval_) {
        sync();
    }
}

void DataFileWriterBase::flush()
{
    sync();
}

boost::mt19937 random(static_cast<uint32_t>(time(0)));

DataFileSync DataFileWriterBase::makeSync()
{
    DataFileSync sync;
    for (size_t i = 0; i < sync.size(); ++i) {
        sync[i] = random();
    }
    return sync;
}

typedef array<uint8_t, 4> Magic;
static Magic magic = { { 'O', 'b', 'j', '\x01' } };

void DataFileWriterBase::writeHeader()
{
    encoderPtr_->init(*stream_);
    avro::encode(*encoderPtr_, magic);
    avro::encode(*encoderPtr_, metadata_);
    avro::encode(*encoderPtr_, sync_);
    encoderPtr_->flush();
}

void DataFileWriterBase::setMetadata(const string& key, const string& value)
{
    vector<uint8_t> v(value.size());
    copy(value.begin(), value.end(), v.begin());
    metadata_[key] = v;
}

DataFileReaderBase::DataFileReaderBase(const char* filename) :
    filename_(filename), stream_(fileInputStream(filename)),
    decoder_(binaryDecoder()), objectCount_(0), eof_(false)
{
    readHeader();
}

void DataFileReaderBase::init()
{
    readerSchema_ = dataSchema_;
    dataDecoder_  = binaryDecoder();
    readDataBlock();
}

void DataFileReaderBase::init(const ValidSchema& readerSchema)
{
    readerSchema_ = readerSchema;
    dataDecoder_  = (toString(readerSchema_) != toString(dataSchema_)) ?
        resolvingDecoder(dataSchema_, readerSchema_, binaryDecoder()) :
        binaryDecoder();
    readDataBlock();
}

static void drain(InputStream& in)
{
    const uint8_t *p = 0;
    size_t n = 0;
    while (in.next(&p, &n));
}

char hex(unsigned int x)
{
    return x + (x < 10 ? '0' :  ('a' - 10));
}

std::ostream& operator << (std::ostream& os, const DataFileSync& s)
{
    for (size_t i = 0; i < s.size(); ++i) {
        os << hex(s[i] / 16)  << hex(s[i] % 16) << ' ';
    }
    os << std::endl;
    return os;
}


bool DataFileReaderBase::hasMore()
{
     if (eof_) {
        return false;
    } else if (objectCount_ != 0) {
        return true;
    }

    dataDecoder_->init(*dataStream_);
    drain(*dataStream_);
    DataFileSync s;
    decoder_->init(*stream_);
    avro::decode(*decoder_, s);
    if (s != sync_) {
        throw Exception("Sync mismatch");
    }
    return readDataBlock();
}

class BoundedInputStream : public InputStream {
    InputStream& in_;
    size_t limit_;

    bool next(const uint8_t** data, size_t* len) {
        if (limit_ != 0 && in_.next(data, len)) {
            if (*len > limit_) {
                in_.backup(*len - limit_);
                *len = limit_;
            }
            limit_ -= *len;
            return true;
        }
        return false;
    }

    void backup(size_t len) {
        in_.backup(len);
        limit_ += len;
    }

    void skip(size_t len) {
        if (len > limit_) {
            len = limit_;
        }
        in_.skip(len);
        limit_ -= len;
    }

    size_t byteCount() const {
        return in_.byteCount();
    }

public:
    BoundedInputStream(InputStream& in, size_t limit) :
        in_(in), limit_(limit) { }
};

auto_ptr<InputStream> boundedInputStream(InputStream& in, size_t limit)
{
    return auto_ptr<InputStream>(new BoundedInputStream(in, limit));
}

bool DataFileReaderBase::readDataBlock()
{
    decoder_->init(*stream_);
    const uint8_t* p = 0;
    size_t n = 0;
    if (! stream_->next(&p, &n)) {
        eof_ = true;
        return false;
    }
    stream_->backup(n);
    avro::decode(*decoder_, objectCount_);
    int64_t byteCount;
    avro::decode(*decoder_, byteCount);
    decoder_->init(*stream_);

    auto_ptr<InputStream> st = boundedInputStream(*stream_, static_cast<size_t>(byteCount));
    if (codec_ == NULL_CODEC) {
        dataDecoder_->init(*st);
        dataStream_ = st;
    } else if (codec_ == DEFLATE_CODEC) {
        compressed_.clear();
        const uint8_t* data;
        size_t len;
        while (st->next(&data, &len)) {
            compressed_.insert(compressed_.end(), data, data + len);
        }
        // boost::iostreams::write(os, reinterpret_cast<const char*>(data), len);
        os_.reset(new boost::iostreams::filtering_istream());
        os_->push(boost::iostreams::zlib_decompressor(get_zlib_params()));
        os_->push(boost::iostreams::basic_array_source<char>(
            &compressed_[0], compressed_.size()));

        std::auto_ptr<InputStream> in = istreamInputStream(*os_);
        dataDecoder_->init(*in);
        dataStream_ = in;
#ifdef SNAPPY_CODEC_AVAILABLE
    } else if (codec_ == SNAPPY_CODEC) {
        boost::crc_32_type crc;
        uint32_t checksum = 0;
        compressed_.clear();
        uncompressed.clear();
        const uint8_t* data;
        size_t len;
        while (st->next(&data, &len)) {
            compressed_.insert(compressed_.end(), data, data + len);
        }
        len = compressed_.size();
        int b1 = compressed_[len - 4] & 0xFF;
        int b2 = compressed_[len - 3] & 0xFF;
        int b3 = compressed_[len - 2] & 0xFF;
        int b4 = compressed_[len - 1] & 0xFF;

        checksum = (b1 << 24) + (b2 << 16) + (b3 << 8) + (b4);
        if (!snappy::Uncompress(reinterpret_cast<const char*>(&compressed_[0]),
                len - 4, &uncompressed)) {
            throw Exception(
                    "Snappy Compression reported an error when decompressing");
        }
        crc.process_bytes(uncompressed.c_str(), uncompressed.size());
        uint32_t c = crc();
        if (checksum != c) {
            throw Exception(boost::format("Checksum did not match for Snappy compression: Expected: %1%, computed: %2%") % checksum % c);
        }
        os_.reset(new boost::iostreams::filtering_istream());
        os_->push(
                boost::iostreams::basic_array_source<char>(uncompressed.c_str(),
                        uncompressed.size()));
        std::auto_ptr<InputStream> in = istreamInputStream(*os_);

        dataDecoder_->init(*in);
        dataStream_ = in;
#endif
    } else {
        throw Exception("Bad codec");
    }
    return true;
}

void DataFileReaderBase::close()
{
}

static string toString(const vector<uint8_t>& v)
{
    string result;
    result.resize(v.size());
    copy(v.begin(), v.end(), result.begin());
    return result;
}

static ValidSchema makeSchema(const vector<uint8_t>& v)
{
    istringstream iss(toString(v));
    ValidSchema vs;
    compileJsonSchema(iss, vs);
    return ValidSchema(vs);
}

void DataFileReaderBase::readHeader()
{
    decoder_->init(*stream_);
    Magic m;
    avro::decode(*decoder_, m);
    if (magic != m) {
        throw Exception("Invalid data file. Magic does not match: "
            + filename_);
    }
    avro::decode(*decoder_, metadata_);
    Metadata::const_iterator it = metadata_.find(AVRO_SCHEMA_KEY);
    if (it == metadata_.end()) {
        throw Exception("No schema in metadata");
    }

    dataSchema_ = makeSchema(it->second);
    if (! readerSchema_.root()) {
        readerSchema_ = dataSchema();
    }

    it = metadata_.find(AVRO_CODEC_KEY);
    if (it != metadata_.end() && toString(it->second) == AVRO_DEFLATE_CODEC) {
        codec_ = DEFLATE_CODEC;
#ifdef SNAPPY_CODEC_AVAILABLE
    } else if (it != metadata_.end()
            && toString(it->second) == AVRO_SNAPPY_CODEC) {
        codec_ = SNAPPY_CODEC;
#endif
    } else {
        codec_ = NULL_CODEC;
        if (it != metadata_.end() && toString(it->second) != AVRO_NULL_CODEC) {
            throw Exception("Unknown codec in data file: " + toString(it->second));
        }
    }

    avro::decode(*decoder_, sync_);
}

}   // namespace avro
