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

import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;

class GradleCompatibility {
    static <T> T createExtensionWithObjectFactory(Project project, String extensionName, Class<T> extensionType) {
        if (GradleFeatures.extensionInjection.isSupported()) {
            return project.getExtensions().create(extensionName, extensionType);
        } else {
            return project.getExtensions().create(extensionName, extensionType, project.getObjects());
        }
    }

    @SuppressWarnings("deprecation")
    static DirectoryProperty createDirectoryProperty(Project project) {
        if (GradleFeatures.objectFactoryDirectoryProperty.isSupported()) {
            return project.getObjects().directoryProperty();
        } else {
            return project.getLayout().directoryProperty();
        }
    }

    @SuppressWarnings("deprecation")
    static ConfigurableFileCollection createConfigurableFileCollection(Project project) {
        if (GradleFeatures.objectFactoryFileCollection.isSupported()) {
            return project.getObjects().fileCollection();
        } else if (GradleFeatures.projectLayoutConfigurableFiles.isSupported()) {
            return project.getLayout().configurableFiles();
        } else {
            return project.files();
        }
    }

    static <T> Property<T> configurePropertyConvention(Property<T> property, Provider<? extends T> valueProvider) {
        if (GradleFeatures.propertyConventions.isSupported()) {
            property.convention(valueProvider);
        } else {
            property.set(valueProvider);
        }
        return property;
    }

    static <T> Property<T> configurePropertyConvention(Property<T> property, T value) {
        if (GradleFeatures.propertyConventions.isSupported()) {
            property.convention(value);
        } else {
            property.set(value);
        }
        return property;
    }

    static <T> ListProperty<T> configureListPropertyConvention(ListProperty<T> property,
                                                               Provider<? extends Iterable<? extends T>> provider) {
        if (GradleFeatures.propertyConventions.isSupported()) {
            property.convention(provider);
        } else {
            property.set(provider);
        }
        return property;
    }

    static <T> ListProperty<T> configureListPropertyConvention(ListProperty<T> property, Iterable<T> elements) {
        if (GradleFeatures.propertyConventions.isSupported()) {
            property.convention(elements);
        } else {
            property.set(elements);
        }
        return property;
    }
}
