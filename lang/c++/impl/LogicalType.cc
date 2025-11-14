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

#include "LogicalType.hh"
#include "Exception.hh"

namespace avro {

LogicalType::LogicalType(Type type)
    : type_(type), precision_(0), scale_(0), custom_(nullptr) {
    if (type == CUSTOM) {
        throw Exception("Logical type CUSTOM must be initialized with a custom logical type");
    }
}

LogicalType::LogicalType(std::shared_ptr<CustomLogicalType> custom)
    : type_(CUSTOM), precision_(0), scale_(0), custom_(std::move(custom)) {}

LogicalType::Type LogicalType::type() const {
    return type_;
}

void LogicalType::setPrecision(int32_t precision) {
    if (type_ != DECIMAL) {
        throw Exception("Only logical type DECIMAL can have precision");
    }
    if (precision <= 0) {
        throw Exception("Precision cannot be: {}", precision);
    }
    precision_ = precision;
}

void LogicalType::setScale(int32_t scale) {
    if (type_ != DECIMAL) {
        throw Exception("Only logical type DECIMAL can have scale");
    }
    if (scale < 0) {
        throw Exception("Scale cannot be: {}", scale);
    }
    scale_ = scale;
}

void LogicalType::printJson(std::ostream &os) const {
    switch (type_) {
        case LogicalType::NONE: break;
        case LogicalType::BIG_DECIMAL:
            os << R"("logicalType": "big-decimal")";
            break;
        case LogicalType::DECIMAL:
            os << R"("logicalType": "decimal")";
            os << ", \"precision\": " << precision_;
            os << ", \"scale\": " << scale_;
            break;
        case DATE:
            os << R"("logicalType": "date")";
            break;
        case TIME_MILLIS:
            os << R"("logicalType": "time-millis")";
            break;
        case TIME_MICROS:
            os << R"("logicalType": "time-micros")";
            break;
        case TIME_NANOS:
            os << R"("logicalType": "time-nanos")";
            break;
        case TIMESTAMP_MILLIS:
            os << R"("logicalType": "timestamp-millis")";
            break;
        case TIMESTAMP_MICROS:
            os << R"("logicalType": "timestamp-micros")";
            break;
        case TIMESTAMP_NANOS:
            os << R"("logicalType": "timestamp-nanos")";
            break;
        case LOCAL_TIMESTAMP_MILLIS:
            os << R"("logicalType": "local-timestamp-millis")";
            break;
        case LOCAL_TIMESTAMP_MICROS:
            os << R"("logicalType": "local-timestamp-micros")";
            break;
        case LOCAL_TIMESTAMP_NANOS:
            os << R"("logicalType": "local-timestamp-nanos")";
            break;
        case DURATION:
            os << R"("logicalType": "duration")";
            break;
        case UUID:
            os << R"("logicalType": "uuid")";
            break;
        case CUSTOM:
            custom_->printJson(os);
            break;
    }
}

void CustomLogicalType::printJson(std::ostream &os) const {
    os << R"("logicalType": ")" << name_ << "\"";
}

CustomLogicalTypeRegistry &CustomLogicalTypeRegistry::instance() {
    static CustomLogicalTypeRegistry instance;
    return instance;
}

void CustomLogicalTypeRegistry::registerType(const std::string &name, Factory factory) {
    std::lock_guard<std::mutex> lock(mutex_);
    registry_[name] = factory;
}

std::shared_ptr<CustomLogicalType> CustomLogicalTypeRegistry::create(const std::string &name, const std::string &json) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = registry_.find(name);
    if (it == registry_.end()) {
        return nullptr;
    }
    return it->second(json);
}

} // namespace avro
