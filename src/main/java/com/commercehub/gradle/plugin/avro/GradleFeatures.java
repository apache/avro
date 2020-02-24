/*
 * Copyright © 2019 Commerce Technologies, LLC.
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

import org.gradle.util.GradleVersion;

enum GradleFeatures {
    extensionInjection() {
        @Override
        boolean isSupported() {
            return GradleVersion.current().compareTo(GradleVersions.v5_2) >= 0;
        }
    },
    objectFactoryFileCollection() {
        @Override
        boolean isSupported() {
            return GradleVersion.current().compareTo(GradleVersions.v5_3) >= 0;
        }
    };

    abstract boolean isSupported();
}
