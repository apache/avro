
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
#include <map>
#include <memory>

#include "CustomAttributes.hh"
#include "Exception.hh"

#include "json/JsonDom.hh"

namespace avro {

CustomAttributes::CustomAttributes(ValueMode valueMode) {
    switch (valueMode) {
    case ValueMode::STRING:
    case ValueMode::JSON:
        valueMode_ = valueMode;
        break;
    default:
        throw Exception("invalid ValueMode: " + std::to_string(static_cast<int>(valueMode)));
    }
}

std::optional<std::string> CustomAttributes::getAttribute(const std::string &name) const {
    auto iter = attributes_.find(name);
    if (iter == attributes_.end()) {
        return {};
    }
    return iter->second;
}

void CustomAttributes::addAttribute(const std::string &name,
                                    const std::string &value) {
    // Validate the incoming value.
    //
    // NOTE: This is a bit annoying that we accept the data as a string instead of
    // as an Entity. That means the compiler must convert the value to a string only
    // for this method to convert it back. But we can't directly refer to the
    // json::Entity type in the signatures for this class (and thus cannot accept
    // that type directly as a parameter) because then it would need to be included
    // from a header file: CustomAttributes.hh. But the json header files are not
    // part of the Avro distribution, so CustomAttributes.hh cannot #include any of
    // the json header files.
    if (valueMode_ == ValueMode::STRING) {
        try {
            json::loadEntity(("\"" + value + "\"").c_str());
        } catch (json::TooManyValuesException e) {
            throw Exception("string has malformed or missing escapes");
        }
    } else {
        json::loadEntity(value.c_str());
    }

    auto iter_and_find =
        attributes_.insert(std::pair<std::string, std::string>(name, value));
    if (!iter_and_find.second) {
        throw Exception(name + " already exists and cannot be added");
    }
}

void CustomAttributes::printJson(std::ostream &os,
                                 const std::string &name) const {
    auto iter = attributes_.find(name);
    if (iter == attributes_.end()) {
        throw Exception(name + " doesn't exist");
    }
    os << json::Entity(std::make_shared<std::string>(name)).toString() << ": ";
    if (valueMode_ == ValueMode::STRING) {
        os << "\"" << iter->second << "\"";
    } else {
        os << iter->second;
    }
}
} // namespace avro
