/**
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

#include "DataFile.hh"
#include "Compiler.hh"
#include "Exception.hh"

#include <random>
#include <sstream>

#ifdef SNAPPY_CODEC_AVAILABLE
#include <snappy.h>
#endif

#ifdef ZSTD_CODEC_AVAILABLE
#include <zstd.h>
#endif

#include <zlib.h>

namespace avro {
using std::copy;
using std::istringstream;
using std::ostringstream;
using std::string;
using std::unique_ptr;
using std::vector;

using std::array;

namespace {
const string AVRO_SCHEMA_KEY("avro.schema");
const string AVRO_CODEC_KEY("avro.codec");
const string AVRO_NULL_CODEC("null");
const string AVRO_DEFLATE_CODEC("deflate");

#ifdef SNAPPY_CODEC_AVAILABLE
const string AVRO_SNAPPY_CODEC = "snappy";
#endif

#ifdef ZSTD_CODEC_AVAILABLE
const string AVRO_ZSTD_CODEC = "zstandard";
#endif

const size_t minSyncInterval = 32;
const size_t maxSyncInterval = 1u << 30;

// Recommended by https://www.zlib.net/zlib_how.html
const size_t zlibBufGrowSize = 128 * 1024;

} // namespace

DataFileWriterBase::DataFileWriterBase(const char *filename, const ValidSchema &schema, size_t syncInterval,
                                       Codec codec, const Metadata &metadata) : filename_(filename),
                                                                                schema_(schema),
                                                                                encoderPtr_(binaryEncoder()),
                                                                                syncInterval_(syncInterval),
                                                                                codec_(codec),
                                                                                stream_(fileOutputStream(filename)),
                                                                                buffer_(memoryOutputStream()),
                                                                                sync_(makeSync()),
                                                                                objectCount_(0),
                                                                                metadata_(metadata),
                                                                                lastSync_(0) {
    init(schema, syncInterval, codec);
}

DataFileWriterBase::DataFileWriterBase(std::unique_ptr<OutputStream> outputStream,
                                       const ValidSchema &schema, size_t syncInterval,
                                       Codec codec, const Metadata &metadata) : filename_(),
                                                                                schema_(schema),
                                                                                encoderPtr_(binaryEncoder()),
                                                                                syncInterval_(syncInterval),
                                                                                codec_(codec),
                                                                                stream_(std::move(outputStream)),
                                                                                buffer_(memoryOutputStream()),
                                                                                sync_(makeSync()),
                                                                                objectCount_(0),
                                                                                metadata_(metadata),
                                                                                lastSync_(0) {
    init(schema, syncInterval, codec);
}

void DataFileWriterBase::init(const ValidSchema &schema, size_t syncInterval, const Codec &codec) {
    if (syncInterval < minSyncInterval || syncInterval > maxSyncInterval) {
        throw Exception(
            "Invalid sync interval: {}. Should be between {} and {}",
            syncInterval, minSyncInterval, maxSyncInterval);
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
#ifdef ZSTD_CODEC_AVAILABLE
    } else if (codec_ == ZSTD_CODEC) {
        setMetadata(AVRO_CODEC_KEY, AVRO_ZSTD_CODEC);
#endif
    } else {
        throw Exception("Unknown codec: {}", int(codec));
    }
    setMetadata(AVRO_SCHEMA_KEY, schema.toJson(false));

    writeHeader();
    encoderPtr_->init(*buffer_);

    lastSync_ = stream_->byteCount();
}

DataFileWriterBase::~DataFileWriterBase() {
    if (stream_) {
        try {
            close();
        } catch (...) {}
    }
}

void DataFileWriterBase::close() {
    flush();
    stream_.reset();
}

void DataFileWriterBase::sync() {
    encoderPtr_->flush();

    encoderPtr_->init(*stream_);
    avro::encode(*encoderPtr_, objectCount_);
    if (codec_ == NULL_CODEC) {
        int64_t byteCount = buffer_->byteCount();
        avro::encode(*encoderPtr_, byteCount);
        encoderPtr_->flush();
        std::unique_ptr<InputStream> in = memoryInputStream(*buffer_);
        copy(*in, *stream_);
    } else if (codec_ == DEFLATE_CODEC) {
        std::vector<uint8_t> buf;
        {
            z_stream zs;
            zs.zalloc = Z_NULL;
            zs.zfree = Z_NULL;
            zs.opaque = Z_NULL;

            int ret = deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY);
            if (ret != Z_OK) {
                throw Exception("Failed to initialize deflate, error: {}", ret);
            }

            std::unique_ptr<InputStream> input = memoryInputStream(*buffer_);
            const uint8_t *data;
            size_t len;
            while (ret != Z_STREAM_END && input->next(&data, &len)) {
                zs.avail_in = static_cast<uInt>(len);
                zs.next_in = const_cast<Bytef *>(data);
                bool flush = (zs.total_in + len) >= buffer_->byteCount();
                do {
                    if (zs.total_out == buf.size()) {
                        buf.resize(buf.size() + zlibBufGrowSize);
                    }
                    zs.avail_out = static_cast<uInt>(buf.size() - zs.total_out);
                    zs.next_out = buf.data() + zs.total_out;
                    ret = deflate(&zs, flush ? Z_FINISH : Z_NO_FLUSH);
                    if (ret == Z_STREAM_END) {
                        break;
                    }
                    if (ret != Z_OK) {
                        throw Exception("Failed to deflate, error: {}", ret);
                    }
                } while (zs.avail_out == 0);
            }

            buf.resize(zs.total_out);
            (void) deflateEnd(&zs);
        } // make sure all is flushed
        std::unique_ptr<InputStream> in = memoryInputStream(buf.data(), buf.size());
        int64_t byteCount = buf.size();
        avro::encode(*encoderPtr_, byteCount);
        encoderPtr_->flush();
        copy(*in, *stream_);
#ifdef SNAPPY_CODEC_AVAILABLE
    } else if (codec_ == SNAPPY_CODEC) {
        std::vector<char> temp;
        std::string compressed;

        const uint8_t *data;
        size_t len;
        std::unique_ptr<InputStream> input = memoryInputStream(*buffer_);
        while (input->next(&data, &len)) {
            temp.insert(temp.end(), reinterpret_cast<const char *>(data),
                        reinterpret_cast<const char *>(data) + len);
        }

        // For Snappy, add the CRC32 checksum
        auto checksum = crc32(0, reinterpret_cast<const Bytef *>(temp.data()),
                              static_cast<uInt>(temp.size()));

        // Now compress
        size_t compressed_size = snappy::Compress(
            reinterpret_cast<const char *>(temp.data()), temp.size(),
            &compressed);

        temp.clear();
        temp.insert(temp.end(), compressed.c_str(),
                    compressed.c_str() + compressed_size);

        temp.push_back(static_cast<char>((checksum >> 24) & 0xFF));
        temp.push_back(static_cast<char>((checksum >> 16) & 0xFF));
        temp.push_back(static_cast<char>((checksum >> 8) & 0xFF));
        temp.push_back(static_cast<char>(checksum & 0xFF));
        std::unique_ptr<InputStream> in = memoryInputStream(
            reinterpret_cast<const uint8_t *>(temp.data()), temp.size());
        int64_t byteCount = temp.size();
        avro::encode(*encoderPtr_, byteCount);
        encoderPtr_->flush();
        copy(*in, *stream_);
#endif
#ifdef ZSTD_CODEC_AVAILABLE
    } else if (codec_ == ZSTD_CODEC) {
        // Read all uncompressed data into a single buffer
        std::vector<char> uncompressed;
        const uint8_t *data;
        size_t len;
        std::unique_ptr<InputStream> input = memoryInputStream(*buffer_);
        while (input->next(&data, &len)) {
            uncompressed.insert(uncompressed.end(), reinterpret_cast<const char *>(data),
                                reinterpret_cast<const char *>(data) + len);
        }

        // Pre-allocate buffer for compressed data
        size_t max_compressed_size = ZSTD_compressBound(uncompressed.size());
        std::vector<char> compressed(max_compressed_size);

        // Compress the data using ZSTD block API
        size_t compressed_size = ZSTD_compress(
            compressed.data(), max_compressed_size,
            uncompressed.data(), uncompressed.size(),
            ZSTD_CLEVEL_DEFAULT);

        if (ZSTD_isError(compressed_size)) {
            throw Exception("ZSTD compression error: {}", ZSTD_getErrorName(compressed_size));
        }

        compressed.resize(compressed_size);
        std::unique_ptr<InputStream> in = memoryInputStream(
            reinterpret_cast<const uint8_t *>(compressed.data()), compressed.size());
        avro::encode(*encoderPtr_, static_cast<int64_t>(compressed_size));
        encoderPtr_->flush();
        copy(*in, *stream_);
#endif
    }

    encoderPtr_->init(*stream_);
    avro::encode(*encoderPtr_, sync_);
    encoderPtr_->flush();

    lastSync_ = stream_->byteCount();

    buffer_ = memoryOutputStream();
    encoderPtr_->init(*buffer_);
    objectCount_ = 0;
}

void DataFileWriterBase::syncIfNeeded() {
    encoderPtr_->flush();
    if (buffer_->byteCount() >= syncInterval_) {
        sync();
    }
}

uint64_t DataFileWriterBase::getCurrentBlockStart() const {
    return lastSync_;
}

void DataFileWriterBase::flush() {
    sync();
}

DataFileSync DataFileWriterBase::makeSync() {
    std::mt19937 random(static_cast<uint32_t>(time(nullptr)));
    DataFileSync sync;
    std::generate(sync.begin(), sync.end(), random);
    return sync;
}

typedef array<uint8_t, 4> Magic;
static Magic magic = {{'O', 'b', 'j', '\x01'}};

void DataFileWriterBase::writeHeader() {
    encoderPtr_->init(*stream_);
    avro::encode(*encoderPtr_, magic);
    avro::encode(*encoderPtr_, metadata_);
    avro::encode(*encoderPtr_, sync_);
    encoderPtr_->flush();
}

void DataFileWriterBase::setMetadata(const string &key, const string &value) {
    vector<uint8_t> v(value.size());
    copy(value.begin(), value.end(), v.begin());
    metadata_[key] = v;
}

DataFileReaderBase::DataFileReaderBase(const char *filename) : filename_(filename), stream_(fileSeekableInputStream(filename)),
                                                               decoder_(binaryDecoder()), objectCount_(0), eof_(false),
                                                               codec_(NULL_CODEC), blockStart_(-1), blockEnd_(-1) {
    readHeader();
}

DataFileReaderBase::DataFileReaderBase(std::unique_ptr<InputStream> inputStream) : stream_(std::move(inputStream)),
                                                                                   decoder_(binaryDecoder()), objectCount_(0), eof_(false), codec_(NULL_CODEC) {
    readHeader();
}

void DataFileReaderBase::init() {
    readerSchema_ = dataSchema_;
    dataDecoder_ = binaryDecoder();
    readDataBlock();
}

void DataFileReaderBase::init(const ValidSchema &readerSchema) {
    readerSchema_ = readerSchema;
    dataDecoder_ = (readerSchema_.toJson(true) != dataSchema_.toJson(true)) ? resolvingDecoder(dataSchema_, readerSchema_, binaryDecoder()) : binaryDecoder();
    readDataBlock();
}

static void drain(InputStream &in) {
    const uint8_t *p = nullptr;
    size_t n = 0;
    while (in.next(&p, &n));
}

char hex(unsigned int x) {
    return static_cast<char>(x + (x < 10 ? '0' : ('a' - 10)));
}

std::ostream &operator<<(std::ostream &os, const DataFileSync &s) {
    for (uint8_t i : s) {
        os << hex(i / 16) << hex(i % 16) << ' ';
    }
    os << std::endl;
    return os;
}

bool DataFileReaderBase::hasMore() {
    for (;;) {
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
        readDataBlock();
    }
}

class BoundedInputStream : public InputStream {
    InputStream &in_;
    size_t limit_;

    bool next(const uint8_t **data, size_t *len) final {
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

    void backup(size_t len) final {
        in_.backup(len);
        limit_ += len;
    }

    void skip(size_t len) final {
        if (len > limit_) {
            len = limit_;
        }
        in_.skip(len);
        limit_ -= len;
    }

    size_t byteCount() const final {
        return in_.byteCount();
    }

public:
    BoundedInputStream(InputStream &in, size_t limit) : in_(in), limit_(limit) {}
};

unique_ptr<InputStream> boundedInputStream(InputStream &in, size_t limit) {
    return unique_ptr<InputStream>(new BoundedInputStream(in, limit));
}

void DataFileReaderBase::readDataBlock() {
    decoder_->init(*stream_);
    blockStart_ = stream_->byteCount();
    const uint8_t *p = nullptr;
    size_t n = 0;
    if (!stream_->next(&p, &n)) {
        eof_ = true;
        return;
    }
    stream_->backup(n);
    avro::decode(*decoder_, objectCount_);
    int64_t byteCount;
    avro::decode(*decoder_, byteCount);
    decoder_->init(*stream_);
    blockEnd_ = stream_->byteCount() + byteCount;

    unique_ptr<InputStream> st = boundedInputStream(*stream_, static_cast<size_t>(byteCount));
    if (codec_ == NULL_CODEC) {
        dataDecoder_->init(*st);
        dataStream_ = std::move(st);
#ifdef SNAPPY_CODEC_AVAILABLE
    } else if (codec_ == SNAPPY_CODEC) {
        uint32_t checksum = 0;
        compressed_.clear();
        uncompressed.clear();
        const uint8_t *data;
        size_t len;
        while (st->next(&data, &len)) {
            compressed_.insert(compressed_.end(), data, data + len);
        }
        len = compressed_.size();
        if (len < 4)
            throw Exception("Cannot read compressed data, expected at least 4 bytes, got " + std::to_string(len));

        int b1 = compressed_[len - 4] & 0xFF;
        int b2 = compressed_[len - 3] & 0xFF;
        int b3 = compressed_[len - 2] & 0xFF;
        int b4 = compressed_[len - 1] & 0xFF;

        checksum = (b1 << 24) + (b2 << 16) + (b3 << 8) + (b4);
        if (!snappy::Uncompress(reinterpret_cast<const char *>(compressed_.data()),
                                len - 4, &uncompressed)) {
            throw Exception(
                "Snappy Compression reported an error when decompressing");
        }
        auto c = crc32(0, reinterpret_cast<const Bytef *>(uncompressed.c_str()),
                       static_cast<uInt>(uncompressed.size()));
        if (checksum != c) {
            throw Exception(
                "Checksum did not match for Snappy compression: Expected: {}, computed: {}",
                checksum, c);
        }

        std::unique_ptr<InputStream> in = memoryInputStream(
            reinterpret_cast<const uint8_t *>(uncompressed.c_str()),
            uncompressed.size());

        dataDecoder_->init(*in);
        dataStream_ = std::move(in);
#endif
#ifdef ZSTD_CODEC_AVAILABLE
    } else if (codec_ == ZSTD_CODEC) {
        compressed_.clear();
        uncompressed.clear();
        const uint8_t *data;
        size_t len;
        while (st->next(&data, &len)) {
            compressed_.insert(compressed_.end(), data, data + len);
        }

        // Get the decompressed size
        size_t decompressed_size = ZSTD_getFrameContentSize(
            reinterpret_cast<const char *>(compressed_.data()), compressed_.size());
        if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
            throw Exception("ZSTD: Not a valid compressed frame");
        } else if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            // Stream decompress the data
            ZSTD_DCtx *dctx = ZSTD_createDCtx();
            if (!dctx) {
                throw Exception("ZSTD decompression error: ZSTD_createDCtx() failed");
            }
            ZSTD_inBuffer in{compressed_.data(), compressed_.size(), 0};
            std::vector<char> tmp(ZSTD_DStreamOutSize());
            ZSTD_outBuffer out{tmp.data(), tmp.size(), 0};
            size_t ret;
            do {
                out.pos = 0;
                ret = ZSTD_decompressStream(dctx, &out, &in);
                if (ZSTD_isError(ret)) {
                    ZSTD_freeDCtx(dctx);
                    throw Exception("ZSTD decompression error: {}", ZSTD_getErrorName(ret));
                }
                uncompressed.append(tmp.data(), out.pos);
            } while (ret != 0);
            ZSTD_freeDCtx(dctx);
        } else {
            // Batch decompress the data
            uncompressed.resize(decompressed_size);
            size_t result = ZSTD_decompress(
                uncompressed.data(), decompressed_size,
                reinterpret_cast<const char *>(compressed_.data()), compressed_.size());

            if (ZSTD_isError(result)) {
                throw Exception("ZSTD decompression error: {}", ZSTD_getErrorName(result));
            }
            if (result != decompressed_size) {
                throw Exception("ZSTD: Decompressed size mismatch: expected {}, got {}",
                                decompressed_size, result);
            }
        }

        std::unique_ptr<InputStream> in = memoryInputStream(
            reinterpret_cast<const uint8_t *>(uncompressed.data()),
            uncompressed.size());

        dataDecoder_->init(*in);
        dataStream_ = std::move(in);
#endif
    } else {
        compressed_.clear();
        uncompressed.clear();

        {
            z_stream zs;
            zs.zalloc = Z_NULL;
            zs.zfree = Z_NULL;
            zs.opaque = Z_NULL;
            zs.avail_in = 0;
            zs.next_in = Z_NULL;

            int ret = inflateInit2(&zs, /*windowBits=*/-15);
            if (ret != Z_OK) {
                throw Exception("Failed to initialize inflate, error: {}", ret);
            }

            const uint8_t *data;
            size_t len;
            while (ret != Z_STREAM_END && st->next(&data, &len)) {
                zs.avail_in = static_cast<uInt>(len);
                zs.next_in = const_cast<Bytef *>(data);
                do {
                    if (zs.total_out == uncompressed.size()) {
                        uncompressed.resize(uncompressed.size() + zlibBufGrowSize);
                    }
                    zs.avail_out = static_cast<uInt>(uncompressed.size() - zs.total_out);
                    zs.next_out = reinterpret_cast<Bytef *>(uncompressed.data() + zs.total_out);
                    ret = inflate(&zs, Z_NO_FLUSH);
                    if (ret == Z_STREAM_END) {
                        break;
                    }
                    if (ret != Z_OK) {
                        throw Exception("Failed to inflate, error: {}", ret);
                    }
                } while (zs.avail_out == 0);
            }

            uncompressed.resize(zs.total_out);
            (void) inflateEnd(&zs);
        }

        std::unique_ptr<InputStream> in = memoryInputStream(
            reinterpret_cast<const uint8_t *>(uncompressed.c_str()),
            uncompressed.size());

        dataDecoder_->init(*in);
        dataStream_ = std::move(in);
    }
}

void DataFileReaderBase::close() {
    stream_.reset();
    eof_ = true;
    objectCount_ = 0;
    blockStart_ = 0;
    blockEnd_ = 0;
}

static string toString(const vector<uint8_t> &v) {
    string result;
    result.resize(v.size());
    copy(v.begin(), v.end(), result.begin());
    return result;
}

static ValidSchema makeSchema(const vector<uint8_t> &v) {
    istringstream iss(toString(v));
    ValidSchema vs;
    compileJsonSchema(iss, vs);
    return vs;
}

void DataFileReaderBase::readHeader() {
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
    if (!readerSchema_.root()) {
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
#ifdef ZSTD_CODEC_AVAILABLE
    } else if (it != metadata_.end() && toString(it->second) == AVRO_ZSTD_CODEC) {
        codec_ = ZSTD_CODEC;
#endif
    } else {
        codec_ = NULL_CODEC;
        if (it != metadata_.end() && toString(it->second) != AVRO_NULL_CODEC) {
            throw Exception("Unknown codec in data file: " + toString(it->second));
        }
    }

    avro::decode(*decoder_, sync_);
    decoder_->init(*stream_);
    blockStart_ = stream_->byteCount();
}

void DataFileReaderBase::doSeek(int64_t position) {
    if (auto *ss = dynamic_cast<SeekableInputStream *>(stream_.get())) {
        if (!eof_) {
            dataDecoder_->init(*dataStream_);
            drain(*dataStream_);
        }
        decoder_->init(*stream_);
        ss->seek(position);
        eof_ = false;
    } else {
        throw Exception("seek not supported on non-SeekableInputStream");
    }
}

void DataFileReaderBase::seek(int64_t position) {
    doSeek(position);
    readDataBlock();
}

void DataFileReaderBase::sync(int64_t position) {
    doSeek(position);
    DataFileSync sync_buffer;
    const uint8_t *p = nullptr;
    size_t n = 0;
    size_t i = 0;
    while (i < SyncSize) {
        if (n == 0 && !stream_->next(&p, &n)) {
            eof_ = true;
            return;
        }
        size_t len = std::min(SyncSize - i, n);
        memcpy(&sync_buffer[i], p, len);
        p += len;
        n -= len;
        i += len;
    }
    for (;;) {
        size_t j = 0;
        for (; j < SyncSize; ++j) {
            if (sync_[j] != sync_buffer[(i + j) % SyncSize]) {
                break;
            }
        }
        if (j == SyncSize) {
            // Found the sync marker!
            break;
        }
        if (n == 0 && !stream_->next(&p, &n)) {
            eof_ = true;
            return;
        }
        sync_buffer[i++ % SyncSize] = *p++;
        --n;
    }
    stream_->backup(n);
    readDataBlock();
}

bool DataFileReaderBase::pastSync(int64_t position) {
    return !hasMore() || blockStart_ >= position + SyncSize;
}

int64_t DataFileReaderBase::previousSync() const {
    return blockStart_;
}

} // namespace avro
