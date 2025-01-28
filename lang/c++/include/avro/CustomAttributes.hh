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
    enum class ValueMode : uint8_t {
        // When a CustomAttributes is created using this mode, all values are expected
        // to be strings. The value should not be quoted, but any interior quotes and
        // special characters (such as newlines) must be escaped.
        STRING
        [[deprecated("The JSON ValueMode is less error-prone and less limited. "
                     "Deprecated since 1.13.0. To be removed with 1.14.0.")]],

        // When a CustomAttributes is created using this mode, all values are formatted
        // JSON values. So string values must be quoted and escaped.
        JSON
    };

    // Creates a new CustomAttributes object where all values are strings.
    // All values passed to addAttribute() and returned from getAttribute() or the
    // attributes() map will not be enclosed in quotes. However, any internal quotes
    // WILL be escaped and other special characters MAY be escaped.
    //
    // To support non-string values, one must instead use
    // CustomAttributes(CustomAttributes::ValueMode::JSON)
    [[deprecated("Use CustomAttributes(ValueMode) instead. Deprecated since 1.13.0")]]
    CustomAttributes() : CustomAttributes(ValueMode::STRING) {}

    // Creates a new CustomAttributes object with the given value mode.
    CustomAttributes(ValueMode valueMode);

    // Retrieves the custom attribute string for the given name. Returns an empty
    // value if the attribute doesn't exist.
    //
    // See ValueMode for details on the format of the returned value.
    std::optional<std::string> getAttribute(const std::string &name) const;

    // Adds a custom attribute.
    //
    // See ValueMode for details on the require format of the value parameter.
    //
    // If the attribute already exists or if the given value is not a valid JSON
    // document or not a correctly escaped string, throw an exception.
    void addAttribute(const std::string &name, const std::string &value);

    // Provides a way to iterate over the custom attributes or check attribute size.
    // The values in this map are the same as those returned from getAttribute.
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
