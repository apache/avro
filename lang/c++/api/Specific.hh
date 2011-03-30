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

#ifndef avro_Codec_hh__
#define avro_Codec_hh__

#include <string>
#include <vector>
#include <map>
#include <algorithm>

#include "boost/array.hpp"

#include "Encoder.hh"
#include "Decoder.hh"

/**
 * A bunch of templates and specializations for encoding and decoding
 * specific types.
 *
 * Primitive AVRO types BOOLEAN, INT, LONG, FLOAT, DOUBLE, STRING and BYTES
 * get decoded to and encoded from C++ types bool, int32_t, int64_t, float,
 * double, std::string and std::vector<uint8_t> respectively. In addition,
 * std::vector<T> for aribtrary type T gets encoded as an Avro array of T.
 * Similarly, std::map<std::string, T> for arbitrary type T gets encoded
 * as an Avro map with value type T.
 * 
 * Users can have their custom types encoded/decoded by specializing
 * avro::codec_traits class for their types.
 */
namespace avro {

template <typename T> void encode(Encoder& e, const T& t);
template <typename T> void decode(Decoder& d, T& t);

template <typename T>
struct codec_traits {
};

template <> struct codec_traits<bool> {
    static void encode(Encoder& e, bool b) {
        e.encodeBool(b);
    }

    static void decode(Decoder& d, bool& b) {
        b = d.decodeBool();
    }
};

template <> struct codec_traits<int32_t> {
    static void encode(Encoder& e, int32_t i) {
        e.encodeInt(i);
    }

    static void decode(Decoder& d, int32_t& i) {
        i = d.decodeInt();
    }
};

template <> struct codec_traits<int64_t> {
    static void encode(Encoder& e, int64_t l) {
        e.encodeLong(l);
    }

    static void decode(Decoder& d, int64_t& l) {
        l = d.decodeLong();
    }
};

template <> struct codec_traits<float> {
    static void encode(Encoder& e, float f) {
        e.encodeFloat(f);
    }

    static void decode(Decoder& d, float& f) {
        f = d.decodeFloat();
    }
};

template <> struct codec_traits<double> {
    static void encode(Encoder& e, double d) {
        e.encodeDouble(d);
    }

    static void decode(Decoder& d, double& dbl) {
        dbl = d.decodeDouble();
    }
};

template <> struct codec_traits<std::string> {
    static void encode(Encoder& e, const std::string& s) {
        e.encodeString(s);
    }

    static void decode(Decoder& d, std::string& s) {
        s = d.decodeString();
    }
};

template <> struct codec_traits<std::vector<uint8_t> > {
    static void encode(Encoder& e, const std::vector<uint8_t>& b) {
        e.encodeBytes(b);
    }

    static void decode(Decoder& d, std::vector<uint8_t>& s) {
        d.decodeBytes(s);
    }
};

template <size_t N> struct codec_traits<boost::array<uint8_t, N> > {
    static void encode(Encoder& e, const boost::array<uint8_t, N>& b) {
        e.encodeFixed(&b[0], N);
    }

    static void decode(Decoder& d, boost::array<uint8_t, N>& s) {
        std::vector<uint8_t> v(N);
        d.decodeFixed(N, v);
        std::copy(&v[0], &v[0] + N, &s[0]);
    }
};

template <typename T> struct codec_traits<std::vector<T> > {
    static void encode(Encoder& e, const std::vector<T>& b) {
        e.arrayStart();
        if (! b.empty()) {
            e.setItemCount(b.size());
            for (typename std::vector<T>::const_iterator it = b.begin();
                it != b.end(); ++it) {
                e.startItem();
                avro::encode(e, *it);
            }
        }
        e.arrayEnd();
    }

    static void decode(Decoder& d, std::vector<T>& s) {
        s.clear();
        for (size_t n = d.arrayStart(); n != 0; n = d.arrayNext()) {
            for (size_t i = 0; i < n; ++i) {
                T t;
                avro::decode(d, t);
                s.push_back(t);
            }
        }
    }
};

template <typename T> struct codec_traits<std::map<std::string, T> > {
    static void encode(Encoder& e, const std::map<std::string, T>& b) {
        e.mapStart();
        if (! b.empty()) {
            e.setItemCount(b.size());
            for (typename std::map<std::string, T>::const_iterator
                it = b.begin();
                it != b.end(); ++it) {
                e.startItem();
                avro::encode(e, it->first);
                avro::encode(e, it->second);
            }
        }
        e.mapEnd();
    }

    static void decode(Decoder& d, std::map<std::string, T>& s) {
        s.clear();
        for (size_t n = d.mapStart(); n != 0; n = d.mapNext()) {
            for (size_t i = 0; i < n; ++i) {
                std::string k;
                avro::decode(d, k);
                T t;
                avro::decode(d, t);
                s[k] = t;
            }
        }
    }
};

template <typename T>
void encode(Encoder& e, const T& t) {
    codec_traits<T>::encode(e, t);
}

template <typename T>
void decode(Decoder& d, T& t) {
    codec_traits<T>::decode(d, t);
}

}   // namespace avro
#endif // avro_Codec_hh__



