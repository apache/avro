/**
 * Copyright © 2013-2019 Commerce Technologies, LLC.
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
package com.commercehub.gradle.plugin.avro;

import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;

public interface AvroExtension {
    Property<String> getOutputCharacterEncoding();
    Property<String> getStringType();
    Property<String> getFieldVisibility();
    Property<String> getTemplateDirectory();
    Property<Boolean> isCreateSetters();
    Property<Boolean> isCreateOptionalGetters();
    Property<Boolean> isGettersReturnOptional();
    Property<Boolean> isEnableDecimalLogicalType();
    Property<String> getDateTimeLogicalType();
    @SuppressWarnings("rawtypes")
    MapProperty<String, Class> getLogicalTypeFactories();
    @SuppressWarnings("rawtypes")
    ListProperty<Class> getCustomConversions();
}
