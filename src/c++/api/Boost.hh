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

#ifndef avro_Boost_hh__
#define avro_Boost_hh__

#include <boost/version.hpp>

#define BOOST_MINOR_VERSION ( BOOST_VERSION / 100 % 1000 )

#if (BOOST_MINOR_VERSION < 33)

/* 
 * In boost 1.33, boost introduced the type trait definitions for true_type and
 * false_type, the pointer containers, and allow any objects to return
 * references.
 *
 * In order to support earlier versions of boost, if these do not exist we just
 * create them here.
 */

#define AVRO_BOOST_NO_ANYREF
#define AVRO_BOOST_NO_TRAIT
#define AVRO_BOOST_NO_PTRVECTOR

#else
#endif

#include <boost/any.hpp>

#ifdef AVRO_BOOST_NO_TRAIT
// this is copied directly from boost documentation
namespace boost {
    template <class T, T val>
    struct integral_constant {
        typedef integral_constant<T, val>  type;
        typedef T                          value_type;
        static const T value = val;
    };

    typedef integral_constant<bool, true>  true_type;
    typedef integral_constant<bool, false> false_type;
} // namespace boost
#else 
#include <boost/type_traits.hpp>
#endif // AVRO_BOOST_NO_TRAIT

#ifdef AVRO_BOOST_NO_PTRVECTOR
#include <vector>
// this implements a minimal subset of ptr_vector (the parts of the API used by avro)
namespace boost {
    template <class T>
    class ptr_vector {
      public:
        ptr_vector() : ptrs_() {}
        ~ptr_vector() {
            for(size_t i=0; i < ptrs_.size(); ++i) {
                delete ptrs_[i];
            }
        }
        void push_back(T *v) {
            ptrs_.push_back(v);
        }
        void pop_back() {
            T *toDelete = ptrs_.back();
            ptrs_.pop_back();
            delete toDelete;
        }
        const T& back() const {
            return *ptrs_.back();
        };
        T& back() {
            return *ptrs_.back();
        };
        bool empty() const {
            return ptrs_.empty();
        }
        const T& at(size_t index) const {
            return *(ptrs_.at(index));
        }
        const T& operator[](size_t index) const {
            return *(ptrs_[index]);
        }
        size_t size() const {
            return ptrs_.size();
        }
      private:
        std::vector<T *> ptrs_;
    };
} // namespace boost
#else 
#include <boost/ptr_container/ptr_vector.hpp>
#endif // AVRO_BOOST_NO_PTRVECTOR

#endif // avro_Boost_hh__
