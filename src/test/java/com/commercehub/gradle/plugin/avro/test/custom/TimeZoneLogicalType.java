/*
 * Copyright © 2019 David M. Carr
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
package com.commercehub.gradle.plugin.avro.test.custom;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class TimeZoneLogicalType extends LogicalType {
    static final TimeZoneLogicalType INSTANCE = new TimeZoneLogicalType();

    private TimeZoneLogicalType() {
        super(TimeZoneConversion.LOGICAL_TYPE_NAME);
    }

    @Override
    public void validate(Schema schema) {
        super.validate(schema);
        if (schema.getType() != Schema.Type.STRING) {
            throw new IllegalArgumentException("Timezone can only be used with an underlying string type");
        }
    }
}
