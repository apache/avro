/*
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

#ifndef avro_CustomAttributes_hh__
#define avro_CustomAttributes_hh__

#include <iostream>
#include <map>
#include <optional>
#include <string>

#include "Config.hh"

namespace avro {

// CustomAttributes class stores avro custom attributes.
// Each attribute is represented by a unique name and value.
// User is supposed to create CustomAttributes object and then add it to Schema.
class AVRO_DECL CustomAttributes {

public:
    // Retrieves the custom attribute value for the given name as a JSON document.
    // Returns an empty value if the attribute doesn't exist. Unlike getAttribute,
    // string values will be quoted and escaped.
    std::optional<std::string> getAttributeJson(const std::string &name) const;

    // Retrieves the custom attribute string for the given name. Returns an empty
    // value if the attribute doesn't exist. If the attribute value is not a string
    // (i.e. it's a number, boolean, array, or object), the stringified form (the
    // value encoded as a JSON document) will be returned.
    std::optional<std::string> getAttribute(const std::string &name) const;

    // Adds a custom attribute with an arbitrary JSON value. The given string must
    // be a valid JSON document. So if the value is a string, it must be quoted
    // and escaped. Unlike addAttribute, this allows setting the value to
    // non-string values, like numbers, booleans, arrays, and objects.
    //
    // If the attribute already exists or if the given value is not a valid JSON
    // document, throw an exception.
    void addAttributeJson(const std::string &name, const std::string &value);

    // Adds a custom attribute with a string value. If the attribute already exists,
    // throw an exception. Unlike with addAttributeJson, the string value must not
    // be encoded (no quoting or escaping).
    void addAttribute(const std::string &name, const std::string &value);

    // Provides a way to iterate over the custom attributes or check attribute size.
    // To query for the json element value of an entry, you can iterate over the keys
    // of this map and then call getAttributeJson for each one.
    const std::map<std::string, std::string> &attributes() const {
        return attributeStrings_;
    }

    // Prints the attribute value for the specific attribute.
    void printJson(std::ostream &os, const std::string &name) const;

private:
    // We have to keep a separate attribute strings map just to implement the
    // attributes() method. This is just for API backwards-compatibility.
    std::map<std::string, std::string> attributeStrings_;

    std::map<std::string, std::string> attributeJson_;
};

} // namespace avro

#endif
