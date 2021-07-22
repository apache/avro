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

package com.github.davidmc24.gradle.plugin.avro;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.tasks.SourceSet;

class GradleCompatibility {
    static <T> T createExtensionWithObjectFactory(Project project, String extensionName, Class<T> extensionType) {
        if (GradleFeatures.extensionInjection.isSupported()) {
            return project.getExtensions().create(extensionName, extensionType);
        } else {
            return project.getExtensions().create(extensionName, extensionType, project.getObjects());
        }
    }

    @SuppressWarnings("deprecation")
    static ConfigurableFileCollection createConfigurableFileCollection(Project project) {
        if (GradleFeatures.objectFactoryFileCollection.isSupported()) {
            return project.getObjects().fileCollection();
        } else {
            Class<?>[] parameterTypes = {Object[].class};
            Object[] args = {new Object[0]};
            return invokeMethod(project.getLayout(), "configurableFiles", parameterTypes, args);
        }
    }

    static String getSourcesJarTaskName(SourceSet sourceSet) {
        if (GradleFeatures.getSourcesJarTaskName.isSupported()) {
            return sourceSet.getSourcesJarTaskName();
        } else {
            return sourceSet.getTaskName(null, "sourcesJar");
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokeMethod(Object object, String methodName, Class<?>[] parameterTypes, Object[] args) {
        try {
            Method method = object.getClass().getMethod(methodName, parameterTypes);
            return (T) method.invoke(object, args);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            throw new RuntimeException("Failed to invoke method via reflection", ex);
        }
    }
}
