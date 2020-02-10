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

import java.util.TimeZone;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

@SuppressWarnings("unused")
public class TimeZoneConversion extends Conversion<TimeZone> {
    public static final String LOGICAL_TYPE_NAME = "timezone";

    @Override
    public Class<TimeZone> getConvertedType() {
        return TimeZone.class;
    }

    @Override
    public String getLogicalTypeName() {
        return LOGICAL_TYPE_NAME;
    }

    @Override
    public TimeZone fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        return TimeZone.getTimeZone(value.toString());
    }

    @Override
    public CharSequence toCharSequence(TimeZone value, Schema schema, LogicalType type) {
        return value.getID();
    }

    @Override
    public Schema getRecommendedSchema() {
        return TimeZoneLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.STRING));
    }
}
