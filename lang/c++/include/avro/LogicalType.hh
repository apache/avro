/*
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

#ifndef avro_LogicalType_hh__
#define avro_LogicalType_hh__

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "Config.hh"

namespace avro {

class CustomLogicalType;

class AVRO_DECL LogicalType {
public:
    enum Type {
        NONE,
        BIG_DECIMAL,
        DECIMAL,
        DATE,
        TIME_MILLIS,
        TIME_MICROS,
        TIMESTAMP_MILLIS,
        TIMESTAMP_MICROS,
        TIMESTAMP_NANOS,
        LOCAL_TIMESTAMP_MILLIS,
        LOCAL_TIMESTAMP_MICROS,
        LOCAL_TIMESTAMP_NANOS,
        DURATION,
        UUID,
        CUSTOM // for registered custom logical types
    };

    explicit LogicalType(Type type);
    explicit LogicalType(std::shared_ptr<CustomLogicalType> custom);

    Type type() const;

    // Precision and scale can only be set for the DECIMAL logical type.
    // Precision must be positive and scale must be either positive or zero. The
    // setters will throw an exception if they are called on any type other
    // than DECIMAL.
    void setPrecision(int32_t precision);
    int32_t precision() const { return precision_; }
    void setScale(int32_t scale);
    int32_t scale() const { return scale_; }

    const std::shared_ptr<CustomLogicalType> &customLogicalType() const {
        return custom_;
    }

    void printJson(std::ostream &os) const;

private:
    Type type_;
    int32_t precision_;
    int32_t scale_;
    std::shared_ptr<CustomLogicalType> custom_;
};

class AVRO_DECL CustomLogicalType {
public:
    CustomLogicalType(const std::string &name) : name_(name) {}

    virtual ~CustomLogicalType() = default;

    const std::string &name() const { return name_; }

    virtual void printJson(std::ostream &os) const;

private:
    std::string name_;
};

// Registry for custom logical types.
// This class is thread-safe.
class AVRO_DECL CustomLogicalTypeRegistry {
public:
    static CustomLogicalTypeRegistry &instance();

    using Factory = std::function<std::shared_ptr<CustomLogicalType>(const std::string &json)>;

    // Register a custom logical type and its factory function.
    void registerType(const std::string &name, Factory factory);

    // Create a custom logical type from a JSON string.
    // Returns nullptr if the name is not registered.
    std::shared_ptr<CustomLogicalType> create(const std::string &name, const std::string &json) const;

private:
    CustomLogicalTypeRegistry() = default;

    std::unordered_map<std::string, Factory> registry_;
    mutable std::mutex mutex_;
};

} // namespace avro

#endif
