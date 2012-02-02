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

#include "Stream.hh"
#include "unistd.h"
#include "fcntl.h"
#include "errno.h"

#ifndef O_BINARY
#define O_BINARY 0
#endif

using std::auto_ptr;
using std::istream;

namespace avro {
namespace stream {
struct BufferCopyIn {
    virtual ~BufferCopyIn() { }
    virtual void seek(size_t len) = 0;
    virtual bool read(uint8_t* b, size_t toRead, size_t& actual) = 0;

};

struct FileBufferCopyIn : public BufferCopyIn {
    const int fd_;

    FileBufferCopyIn(const char* filename) :
        fd_(open(filename, O_RDONLY | O_BINARY)) {
        if (fd_ < 0) {
            throw Exception(boost::format("Cannot open file: %1%") %
                ::strerror(errno));
        }
    }

    ~FileBufferCopyIn() {
        ::close(fd_);
    }

    void seek(size_t len) {
        off_t r = ::lseek(fd_, len, SEEK_CUR);
        if (r == static_cast<off_t>(-1)) {
            throw Exception(boost::format("Cannot skip file: %1%") %
                strerror(errno));
        }
    }

    bool read(uint8_t* b, size_t toRead, size_t& actual) {
        int n = ::read(fd_, b, toRead);
        if (n > 0) {
            actual = n;
            return true;
        }
        return false;
    }

};

struct IStreamBufferCopyIn : public BufferCopyIn {
    istream& is_;

    IStreamBufferCopyIn(istream& is) : is_(is) {
    }

    void seek(size_t len) {
        if (! is_.seekg(len, std::ios_base::cur)) {
            throw Exception("Cannot skip stream");
        }
    }

    bool read(uint8_t* b, size_t toRead, size_t& actual) {
        is_.read(reinterpret_cast<char*>(b), toRead);
        if (is_.bad()) {
            return false;
        }
        actual = is_.gcount();
        return (! is_.eof() || actual != 0);
    }

};

}

class BufferCopyInInputStream : public InputStream {
    const size_t bufferSize_;
    uint8_t* const buffer_;
    auto_ptr<stream::BufferCopyIn> in_;
    size_t byteCount_;
    uint8_t* next_;
    size_t available_;

    bool next(const uint8_t** data, size_t *size) {
        if (available_ == 0 && ! fill()) {
            return false;
        }
        *data = next_;
        *size = available_;
        next_ += available_;
        byteCount_ += available_;
        available_ = 0;
        return true;
    }

    void backup(size_t len) {
        next_ -= len;
        available_ += len;
        byteCount_ -= len;
    }

    void skip(size_t len) {
        while (len > 0) {
            if (available_ == 0) {
                in_->seek(len);
                byteCount_ += len;
                return;
            }
            size_t n = std::min(available_, len);
            available_ -= n;
            next_ += n;
            len -= n;
            byteCount_ += n;
        }
    }

    size_t byteCount() const { return byteCount_; }

    bool fill() {
        size_t n = 0;
        if (in_->read(buffer_, bufferSize_, n)) {
            next_ = buffer_;
            available_ = n;
            return true;
        }
        return false;
    }


public:
    BufferCopyInInputStream(auto_ptr<stream::BufferCopyIn>& in, size_t bufferSize) :
        bufferSize_(bufferSize),
        buffer_(new uint8_t[bufferSize]),
        in_(in),
        byteCount_(0),
        next_(buffer_),
        available_(0) { }

    ~BufferCopyInInputStream() {
        delete[] buffer_;
    }
};

class FileOutputStream : public OutputStream {
    size_t bufferSize_;
    uint8_t* const buffer_;
    int out_;
    uint8_t* next_;
    size_t available_;
    size_t byteCount_;

    // Invaiant: byteCount_ == byteswritten + bufferSize_ - available_;
    bool next(uint8_t** data, size_t* len) {
        if (available_ == 0) {
            flush();
        }
        *data = next_;
        *len = available_;
        next_ += available_;
        byteCount_ += available_;
        available_ = 0;
        return true;
    }

    void backup(size_t len) {
        available_ += len;
        next_ -= len;
        byteCount_ -= len;
    }

    uint64_t byteCount() const {
        return byteCount_;
    }

    void flush() {
        if (::write(out_, buffer_, bufferSize_ - available_) < 0) {
            throw Exception(boost::format("Cannot write file: %1%") %
                strerror(errno));
        }
        next_ = buffer_;
        available_ = bufferSize_;
    }

public:
    FileOutputStream(const char* filename, size_t bufferSize) :
        bufferSize_(bufferSize),
        buffer_(new uint8_t[bufferSize]),
        out_(::open(filename, O_WRONLY | O_CREAT | O_BINARY, 0644)),
        next_(buffer_),
        available_(bufferSize_), byteCount_(0) { }

    ~FileOutputStream() {
        if (out_ >= 0) {
            ::close(out_);
        }
        delete[] buffer_;
    }
};

auto_ptr<InputStream> fileInputStream(const char* filename,
    size_t bufferSize)
{
    auto_ptr<stream::BufferCopyIn> in(new stream::FileBufferCopyIn(filename));
    return auto_ptr<InputStream>( new BufferCopyInInputStream(in, bufferSize));
}

auto_ptr<InputStream> istreamInputStream(istream& is,
    size_t bufferSize)
{
    auto_ptr<stream::BufferCopyIn> in(new stream::IStreamBufferCopyIn(is));
    return auto_ptr<InputStream>( new BufferCopyInInputStream(in, bufferSize));
}

auto_ptr<OutputStream> fileOutputStream(const char* filename,
    size_t bufferSize)
{
    return auto_ptr<OutputStream>(
        new FileOutputStream(filename, bufferSize));
}

}   // namespace avro
