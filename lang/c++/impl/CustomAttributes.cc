
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

std::optional<std::string> CustomAttributes::getAttribute(const std::string &name) const {
    auto iter = attributeStrings_.find(name);
    if (iter == attributeStrings_.end()) {
        return {};
    }
    return iter->second;
}

std::optional<std::string> CustomAttributes::getAttributeJson(const std::string &name) const {
    auto iter = attributeJson_.find(name);
    if (iter == attributeJson_.end()) {
        return {};
    }
    return iter->second;
}

void CustomAttributes::addAttribute(const std::string &name,
                                    const std::string &value) {
    auto iter_and_find =
        attributeStrings_.insert(std::pair<std::string, std::string>(name, value));
    if (!iter_and_find.second) {
        throw Exception(name + " already exists and cannot be added");
    }
    attributeJson_[name] = json::Entity(std::make_shared<std::string>(value)).toString();
}

void CustomAttributes::addAttributeJson(const std::string &name,
                                        const std::string &value) {
    // NOTE: This is a bit annoying that we accept the data as a string instead of
    // as an Entity. That means the compiler must convert the value to a string only
    // for this method to convert it back. But we can't directly refer to the
    // json::Entity type in the signatures for this class (and thus cannot accept
    // that type directly as a parameter) because then it would need to be included
    // from a header file: CustomAttributes.hh. But the json header files are not
    // part of the Avro distribution, so these header files cannot #include any of
    // the json header files.
    json::Entity e = json::loadEntity(value.c_str());
    auto iter_and_find =
        attributeJson_.insert(std::pair<std::string, std::string>(name, value));
    if (!iter_and_find.second) {
        throw Exception(name + " already exists and cannot be added");
    }
    attributeStrings_[name] = e.type() == json::EntityType::String ? e.stringValue() : value;
}

void CustomAttributes::printJson(std::ostream &os,
                                 const std::string &name) const {
    auto iter = attributeJson_.find(name);
    if (iter == attributeJson_.end()) {
        throw Exception(name + " doesn't exist");
    }
    os << json::Entity(std::make_shared<std::string>(name)).toString() << ": " << iter->second;
}
} // namespace avro
