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

#ifndef avro_Stream_hh__
#define avro_Stream_hh__

#include <memory>
#include <string.h>
#include <stdint.h>
#include "boost/utility.hpp"
#include "Exception.hh"

namespace avro {
class InputStream : boost::noncopyable {
public:
    InputStream() { }
    virtual ~InputStream() { }

    /**
     * Returns some of available data.
     *
     * Returns true if some data is available, false if no more data is
     * available or an error has occurred.
     */
    virtual bool next(const uint8_t** data, size_t* len) = 0;

    /**
     * "Returns" back some of the data to the stream. The returned
     * data must be less than what was obtained in the last call to
     * next().
     */
    virtual void backup(size_t len) = 0;

    /**
     * Skips number of bytes specified by len.
     */
    virtual void skip(size_t len) = 0;

    /**
     * Returns the number of bytes read from this stream so far.
     * All the bytes made available through next are considered
     * to be used unless, retutned back using backup.
     */
    virtual size_t byteCount() const = 0;
};

class OutputStream : boost::noncopyable {
public:
    OutputStream() { }
    virtual ~OutputStream() { }

    /**
     * Returns a buffer that can be written into.
     * On successful return, data has the pointer to the buffer
     * and len has the number of bytes available at data.
     */
    virtual bool next(uint8_t** data, size_t* len) = 0;

    /**
     * "Returns" back to the stream some of the buffer obtained
     * from in the last call to next().
     */
    virtual void backup(size_t len) = 0;

    /**
     * Number of bytes written so far into this stream. The whole buffer
     * returned by next() is assumed to be written unless some of
     * it was retutned using backup().
     */
    virtual uint64_t byteCount() const = 0;

    /**
     * Flushes any data remaining in the buffer to the stream's underlying
     * store, if any.
     */
    virtual void flush() = 0;
};

/**
 * Returns a new OutputStream, which grows in memory chunks of specified size.
 */
std::auto_ptr<OutputStream> memoryOutputStream(size_t chunkSize = 4 * 1024);

/**
 * Returns a new InputStream, with the data from the given byte array.
 * It does not copy the data, the byte array should remain valid
 * until the InputStream is used.
 */
std::auto_ptr<InputStream> memoryInputStream(const uint8_t* data, size_t len);

/**
 * Returns a new InputStream with the contents written into an
 * outputstream. The output stream must have been returned by
 * an earlier call to memoryOutputStream(). The contents for the new
 * input stream are the snapshot of the outputstream. One can construct
 * any number of memory input stream from a single memory output stream.
 */
std::auto_ptr<InputStream> memoryInputStream(const OutputStream& source);

/**
 * Returns a new OutputStream whose contents would be stored in a file.
 * Data is written in chunks of given buffer size.
 *
 * If there is a file with the given name, it is truncated and overwritten.
 * If there is no file with the given name, it is created.
 */
std::auto_ptr<OutputStream> fileOutputStream(const char* filename,
    size_t bufferSize = 8 * 1024);

/**
 * Returns a new InputStream whose contents come from the given file.
 * Data is read in chunks of given buffer size.
 */
std::auto_ptr<InputStream> fileInputStream(const char* filename,
    size_t bufferSize = 8 * 1024);

/** A convenience class for reading from an InputStream */
struct StreamReader {
    InputStream* in_;
    const uint8_t* next_;
    const uint8_t* end_;

    StreamReader() : in_(0), next_(0), end_(0) { }
    StreamReader(InputStream& in) : in_(0), next_(0), end_(0) { reset(in); }

    void reset(InputStream& is) {
        if (in_ != 0) {
            in_->backup(end_ - next_);
        }
        in_ = &is;
        next_ = end_ = 0;
    }

    uint8_t read() {
        if (next_ == end_) {
            more();
        }
        return *next_++;
    }

    void readBytes(uint8_t* b, size_t n) {
        while (n > 0) {
            if (next_ == end_) {
                more();
            }
            size_t q = end_ - next_;
            if (q > n) {
                q = n;
            }
            ::memcpy(b, next_, q);
            next_ += q;
            b += q;
            n -= q;
        }
    }

    void skipBytes(size_t n) {
        if (n > (end_ - next_)) {
            n -= end_ - next_;
            next_ = end_;
            in_->skip(n);
        } else {
            next_ += n;
        }
    }

    bool fill() {
        size_t n = 0;
        while (in_->next(&next_, &n)) {
            if (n != 0) {
                end_ = next_ + n;
                return true;
            }
        }
        return false;
    }

    void more() {
        if (! fill()) {
            throw Exception("EOF reached");
        }
    }

    bool hasMore() {
        return (next_ == end_) ? fill() : true;
    }
};

/**
 * A convinience class to write data into an OutputStream.
 */
struct StreamWriter {
    OutputStream* out_;
    uint8_t* next_;
    uint8_t* end_;

    StreamWriter() : out_(0), next_(0), end_(0) { }
    StreamWriter(OutputStream& out) : out_(0), next_(0), end_(0) { reset(out); }

    void reset(OutputStream& os) {
        if (out_ != 0) {
            out_->backup(end_ - next_);
        }
        out_ = &os;
        next_ = end_;
    }

    void write(uint8_t c) {
        if (next_ == end_) {
            more();
        }
        *next_++ = c;
    }

    void writeBytes(const uint8_t* b, size_t n) {
        while (n > 0) {
            if (next_ == end_) {
                more();
            }
            size_t q = end_ - next_;
            if (q > n) {
                q = n;
            }
            ::memcpy(next_, b, q);
            next_ += q;
            b += q;
            n -= q;
        }
    }

    void more() {
        size_t n = 0;
        while (out_->next(&next_, &n)) {
            if (n != 0) {
                end_ = next_ + n;
                return;
            }
        }
        throw Exception("EOF reached");
    }

    void flush() {
        if (next_ != end_) {
            out_->backup(end_ - next_);
            next_ = end_;
        }
        out_->flush();
    }
};

/**
 * A convenience function to copy all the contents of an input stream into
 * an output stream.
 */
inline void copy(InputStream& in, OutputStream& out)
{
    const uint8_t *p = 0;
    size_t n = 0;
    StreamWriter w(out);
    while (in.next(&p, &n)) {
        w.writeBytes(p, n);
    }
    w.flush();
}

}   // namespace avro
#endif


