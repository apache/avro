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
    enum ValueMode {
        // When a CustomAttributes is created using this mode, all values are strings.
        // The value should not be quoted, but any interior quotes and special
        // characters (such as newlines) must be escaped.
        string,
        // When a CustomAttributes is created using this mode, all values are JSON
        // values. String values must be quoted and escaped.
        json
    };

    // Creates a new CustomAttributes object where all values are strings.
    // All values passed to addAttribute() and returned from getAttribute() or the
    // attributes() map will not be enclosed in quotes. However, any internal quotes
    // WILL be escaped and other special characters MAY be escaped.
    //
    // To support non-string values, use CustomAttributes(CustomAttributes::json) instead.
    CustomAttributes() : CustomAttributes(string) {}

    // Creates a new CustomAttributes object.
    //
    // If the given mode is string, all values must be strings. All values passed to
    // addAttribute() and returned from getAttribute() or the attributes() map will not
    // be enclosed in quotes. However, any internal quotes and newlines WILL be escaped
    // and other special characters MAY be escaped.
    //
    // If the given mode is json, the values support any valid JSON type. In this more,
    // all values passed to addAttribute() and returned from getAttribute() or the
    // attributes() map must be valid JSON values; string values must be quoted and
    // escaped.
    CustomAttributes(ValueMode valueMode);

    // Retrieves the custom attribute string for the given name. Returns an empty
    // value if the attribute doesn't exist.
    //
    // If this CustomAttributes was created in json mode, the returned string will
    // be a JSON document (and string values will be quoted and escaped). Otherwise,
    // the returned string will be a string value, though interior quotes and newlines
    // will be escaped and other special characters may be escaped.
    std::optional<std::string> getAttribute(const std::string &name) const;

    // Adds a custom attribute.
    //
    // If this CustomAttributes was create in json mode, the given value string must
    // be a valid JSON document. So if the value is a string, it must be quoted and
    // escaped. Otherwise, the given value string is an unquoted string value (though
    // interior quotes and newlines must still be escaped).
    //
    // If the attribute already exists or if the given value is not a valid JSON
    // document or not a correctly escaped string, throw an exception.
    void addAttribute(const std::string &name, const std::string &value);

    // Provides a way to iterate over the custom attributes or check attribute size.
    // The values in this map are the same as those returned from getAttribute. So
    // the value may be a JSON document or an unquoted string, depending on whether
    // this CustomAttributes was created in json or string mode.
    const std::map<std::string, std::string> &attributes() const {
        return attributes_;
    }

    // Prints the attribute value for the specific attribute.
    void printJson(std::ostream &os, const std::string &name) const;

private:
    ValueMode valueMode_;
    std::map<std::string, std::string> attributes_;
};

} // namespace avro

#endif
