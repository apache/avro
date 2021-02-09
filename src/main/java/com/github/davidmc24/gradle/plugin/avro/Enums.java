/**
 * Copyright © 2015 Commerce Technologies, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.davidmc24.gradle.plugin.avro;

import java.util.Arrays;

class Enums {
    static <T extends Enum<T>> T parseCaseInsensitive(String label, T[] values, String input) {
        for (T value : values) {
            if (value.name().equalsIgnoreCase(input)) {
                return value;
            }
        }
        throw new IllegalArgumentException(String.format("Invalid %s '%s'.  Value values are: %s",
                label, input, Arrays.asList(values)));
    }
}
