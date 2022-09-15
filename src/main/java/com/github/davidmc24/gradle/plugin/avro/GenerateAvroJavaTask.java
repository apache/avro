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
package com.github.davidmc24.gradle.plugin.avro;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.StringType;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.specs.NotSpec;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;

/**
 * Task to generate Java source files based on Avro protocol files and Avro schema files using {@link Protocol} and
 * {@link SpecificCompiler}.
 */
@SuppressWarnings("WeakerAccess")
@CacheableTask
public class GenerateAvroJavaTask extends OutputDirTask {
    private static Set<String> SUPPORTED_EXTENSIONS =
        new SetBuilder<String>().add(Constants.PROTOCOL_EXTENSION).add(Constants.SCHEMA_EXTENSION).build();

    private final Property<String> outputCharacterEncoding;
    private final Property<String> stringType;
    private final Property<String> fieldVisibility;
    private final Property<String> templateDirectory;
    private final ListProperty<String> additionalVelocityToolClasses;
    private final Property<Boolean> createOptionalGetters;
    private final Property<Boolean> gettersReturnOptional;
    private final Property<Boolean> optionalGettersForNullableFieldsOnly;
    private final Property<Boolean> createSetters;
    private final Property<Boolean> enableDecimalLogicalType;
    private final MapProperty<String, Class<? extends LogicalTypes.LogicalTypeFactory>> logicalTypeFactories;
    private final ListProperty<Class<? extends Conversion<?>>> customConversions;

    private final Provider<StringType> stringTypeProvider;
    private final Provider<FieldVisibility> fieldVisibilityProvider;

    private final ProjectLayout projectLayout;
    private final SchemaResolver resolver;

    @Inject
    public GenerateAvroJavaTask(ObjectFactory objects) {
        super();
        this.outputCharacterEncoding = objects.property(String.class);
        this.stringType = objects.property(String.class).convention(Constants.DEFAULT_STRING_TYPE);
        this.fieldVisibility = objects.property(String.class).convention(Constants.DEFAULT_FIELD_VISIBILITY);
        this.templateDirectory = objects.property(String.class);
        this.additionalVelocityToolClasses =
                objects.listProperty(String.class).convention(Collections.emptyList());
        this.createOptionalGetters = objects.property(Boolean.class).convention(Constants.DEFAULT_CREATE_OPTIONAL_GETTERS);
        this.gettersReturnOptional = objects.property(Boolean.class).convention(Constants.DEFAULT_GETTERS_RETURN_OPTIONAL);
        this.optionalGettersForNullableFieldsOnly = objects.property(Boolean.class)
            .convention(Constants.DEFAULT_OPTIONAL_GETTERS_FOR_NULLABLE_FIELDS_ONLY);
        this.createSetters = objects.property(Boolean.class).convention(Constants.DEFAULT_CREATE_SETTERS);
        this.enableDecimalLogicalType = objects.property(Boolean.class).convention(Constants.DEFAULT_ENABLE_DECIMAL_LOGICAL_TYPE);
        this.stringTypeProvider = getStringType()
            .map(input -> Enums.parseCaseInsensitive(Constants.OPTION_STRING_TYPE, StringType.values(), input));
        this.fieldVisibilityProvider = getFieldVisibility()
            .map(input -> Enums.parseCaseInsensitive(Constants.OPTION_FIELD_VISIBILITY, FieldVisibility.values(), input));
        this.logicalTypeFactories = objects.mapProperty(String.class, Constants.LOGICAL_TYPE_FACTORY_TYPE.getConcreteClass())
            .convention(Constants.DEFAULT_LOGICAL_TYPE_FACTORIES);
        this.customConversions =
            objects.listProperty(Constants.CONVERSION_TYPE.getConcreteClass()).convention(Constants.DEFAULT_CUSTOM_CONVERSIONS);
        this.projectLayout = getProject().getLayout();
        this.resolver = new SchemaResolver(projectLayout, getLogger());
    }

    @Optional
    @Input
    public Property<String> getOutputCharacterEncoding() {
        return outputCharacterEncoding;
    }

    public void setOutputCharacterEncoding(String outputCharacterEncoding) {
        this.outputCharacterEncoding.set(outputCharacterEncoding);
    }

    public void setOutputCharacterEncoding(Charset outputCharacterEncoding) {
        setOutputCharacterEncoding(outputCharacterEncoding.name());
    }

    @Input
    public Property<String> getStringType() {
        return stringType;
    }

    public void setStringType(GenericData.StringType stringType) {
        setStringType(stringType.name());
    }

    public void setStringType(String stringType) {
        this.stringType.set(stringType);
    }

    @Input
    public Property<String> getFieldVisibility() {
        return fieldVisibility;
    }

    public void setFieldVisibility(String fieldVisibility) {
        this.fieldVisibility.set(fieldVisibility);
    }

    public void setFieldVisibility(SpecificCompiler.FieldVisibility fieldVisibility) {
        setFieldVisibility(fieldVisibility.name());
    }

    @Optional
    @Input
    public Property<String> getTemplateDirectory() {
        return templateDirectory;
    }

    public void setTemplateDirectory(String templateDirectory) {
        this.templateDirectory.set(templateDirectory);
    }

    @Optional
    @Input
    public ListProperty<String> getAdditionalVelocityToolClasses() {
        return additionalVelocityToolClasses;
    }

    public void setAdditionalVelocityToolClasses(List<String> additionalVelocityToolClasses) {
        this.additionalVelocityToolClasses.set(additionalVelocityToolClasses);
    }

    public Property<Boolean> isCreateSetters() {
        return createSetters;
    }

    @Input
    public Property<Boolean> getCreateSetters() {
        return createSetters;
    }

    public void setCreateSetters(String createSetters) {
        this.createSetters.set(Boolean.parseBoolean(createSetters));
    }

    public Property<Boolean> isCreateOptionalGetters() {
        return createOptionalGetters;
    }

    @Input
    public Property<Boolean> getCreateOptionalGetters() {
        return createOptionalGetters;
    }

    public void setCreateOptionalGetters(String createOptionalGetters) {
        this.createOptionalGetters.set(Boolean.parseBoolean(createOptionalGetters));
    }

    public Property<Boolean> isGettersReturnOptional() {
        return gettersReturnOptional;
    }

    @Input
    public Property<Boolean> getGettersReturnOptional() {
        return gettersReturnOptional;
    }

    public void setGettersReturnOptional(String gettersReturnOptional) {
        this.gettersReturnOptional.set(Boolean.parseBoolean(gettersReturnOptional));
    }

    public Property<Boolean> isOptionalGettersForNullableFieldsOnly() {
        return optionalGettersForNullableFieldsOnly;
    }

    @Input
    public Property<Boolean> getOptionalGettersForNullableFieldsOnly() {
        return optionalGettersForNullableFieldsOnly;
    }

    public void setOptionalGettersForNullableFieldsOnly(String optionalGettersForNullableFieldsOnly) {
        this.optionalGettersForNullableFieldsOnly.set(Boolean.parseBoolean(optionalGettersForNullableFieldsOnly));
    }

    public Property<Boolean> isEnableDecimalLogicalType() {
        return enableDecimalLogicalType;
    }

    @Input
    public Property<Boolean> getEnableDecimalLogicalType() {
        return enableDecimalLogicalType;
    }

    public void setEnableDecimalLogicalType(String enableDecimalLogicalType) {
        this.enableDecimalLogicalType.set(Boolean.parseBoolean(enableDecimalLogicalType));
    }

    @Optional
    @Input
    public MapProperty<String, Class<? extends LogicalTypes.LogicalTypeFactory>> getLogicalTypeFactories() {
        return logicalTypeFactories;
    }

    public void setLogicalTypeFactories(Provider<? extends Map<? extends String,
        ? extends Class<? extends LogicalTypes.LogicalTypeFactory>>> provider) {
        this.logicalTypeFactories.set(provider);
    }

    public void setLogicalTypeFactories(Map<? extends String,
        ? extends Class<? extends LogicalTypes.LogicalTypeFactory>> logicalTypeFactories) {
        this.logicalTypeFactories.set(logicalTypeFactories);
    }

    @Optional
    @Input
    public ListProperty<Class<? extends Conversion<?>>> getCustomConversions() {
        return customConversions;
    }

    public void setCustomConversions(Provider<Iterable<Class<? extends Conversion<?>>>> provider) {
        this.customConversions.set(provider);
    }

    public void setCustomConversions(Iterable<Class<? extends Conversion<?>>> customConversions) {
        this.customConversions.set(customConversions);
    }

    @TaskAction
    protected void process() {
        getLogger().debug("Using outputCharacterEncoding {}", getOutputCharacterEncoding().getOrNull());
        getLogger().debug("Using stringType {}", stringTypeProvider.get().name());
        getLogger().debug("Using fieldVisibility {}", fieldVisibilityProvider.get().name());
        getLogger().debug("Using templateDirectory '{}'", getTemplateDirectory().getOrNull());
        getLogger().debug("Using additionalVelocityToolClasses '{}'", getAdditionalVelocityToolClasses().getOrNull());
        getLogger().debug("Using createSetters {}", isCreateSetters().get());
        getLogger().debug("Using createOptionalGetters {}", isCreateOptionalGetters().get());
        getLogger().debug("Using gettersReturnOptional {}", isGettersReturnOptional().get());
        getLogger().debug("Using optionalGettersForNullableFieldsOnly {}", isOptionalGettersForNullableFieldsOnly().get());
        getLogger().debug("Using enableDecimalLogicalType {}", isEnableDecimalLogicalType().get());
        getLogger().debug("Using logicalTypeFactories {}",
            logicalTypeFactories.get().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                (Map.Entry<String, Class<? extends LogicalTypes.LogicalTypeFactory>> e) -> e.getValue().getName()
            )));
        getLogger().debug("Using customConversions {}",
            customConversions.get().stream().map(v -> ((Class) v).getName()).collect(Collectors.toList()));
        getLogger().info("Found {} files", getInputs().getSourceFiles().getFiles().size());
        failOnUnsupportedFiles();
        processFiles();
    }

    private void failOnUnsupportedFiles() {
        FileCollection unsupportedFiles = filterSources(new NotSpec<>(new FileExtensionSpec(SUPPORTED_EXTENSIONS)));
        if (!unsupportedFiles.isEmpty()) {
            throw new GradleException(
                String.format("Unsupported file extension for the following files: %s", unsupportedFiles));
        }
    }

    private void processFiles() {
        registerLogicalTypes();
        int processedFileCount = 0;
        processedFileCount += processProtoFiles();
        processedFileCount += processSchemaFiles();
        setDidWork(processedFileCount > 0);
    }

    private int processProtoFiles() {
        int processedFileCount = 0;
        for (File sourceFile : filterSources(new FileExtensionSpec(Constants.PROTOCOL_EXTENSION))) {
            processProtoFile(sourceFile);
            processedFileCount++;
        }
        return processedFileCount;
    }

    private void processProtoFile(File sourceFile) {
        getLogger().info("Processing {}", sourceFile);
        try {
            compile(new SpecificCompiler(Protocol.parse(sourceFile)), sourceFile);
        } catch (IOException ex) {
            throw new GradleException(String.format("Failed to compile protocol definition file %s", sourceFile), ex);
        }
    }

    private int processSchemaFiles() {
        Set<File> files = filterSources(new FileExtensionSpec(Constants.SCHEMA_EXTENSION)).getFiles();
        ProcessingState processingState = resolver.resolve(files);
        for (File file : files) {
            String path = FileUtils.projectRelativePath(projectLayout, file);
            for (Schema schema : processingState.getSchemasForLocation(path)) {
                try {
                    compile(new SpecificCompiler(schema), file);
                } catch (IOException ex) {
                    throw new GradleException(String.format("Failed to compile schema definition file %s", path), ex);
                }
            }
        }
        return processingState.getProcessedTotal();
    }

    private void compile(SpecificCompiler compiler, File sourceFile) throws IOException {
        compiler.setOutputCharacterEncoding(getOutputCharacterEncoding().getOrNull());
        compiler.setStringType(stringTypeProvider.get());
        compiler.setFieldVisibility(fieldVisibilityProvider.get());
        if (getTemplateDirectory().isPresent()) {
            compiler.setTemplateDir(getTemplateDirectory().get());
        }
        if (getAdditionalVelocityToolClasses().isPresent()) {
            List<Object> tools = getAdditionalVelocityToolClasses().get().stream()
                    .map(s -> {
                        try {
                            return Class.forName(s);
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException("unable to load velocity tool class " + s, e);
                        }
                    })
                    .map(aClass -> {
                        try {
                            return aClass.getConstructor().newInstance();
                        } catch (InstantiationException
                                 | NoSuchMethodException
                                 | InvocationTargetException
                                 | IllegalAccessException e) {
                            throw new RuntimeException("Unable to instantiate velocity tool class using default constructor: " + aClass, e);
                        }
                    }).collect(Collectors.toList());
            compiler.setAdditionalVelocityTools(tools);
        }
        compiler.setCreateOptionalGetters(createOptionalGetters.get());
        compiler.setGettersReturnOptional(gettersReturnOptional.get());
        compiler.setOptionalGettersForNullableFieldsOnly(optionalGettersForNullableFieldsOnly.get());
        compiler.setCreateSetters(isCreateSetters().get());
        compiler.setEnableDecimalLogicalType(isEnableDecimalLogicalType().get());
        registerCustomConversions(compiler);

        compiler.compileToDestination(sourceFile, getOutputDir().get().getAsFile());
    }

    /**
     * Registers the logical types to be used in this run.
     * This must be called before the Schemas are parsed, or they will not be applied correctly.
     * Since {@link LogicalTypes} is a static registry, this may result in side-effects.
     */
    private void registerLogicalTypes() {
        Map<String, Class<? extends LogicalTypes.LogicalTypeFactory>> logicalTypeFactoryMap = logicalTypeFactories.get();
        Set<Map.Entry<String, Class<? extends LogicalTypes.LogicalTypeFactory>>> logicalTypeFactoryEntries =
            logicalTypeFactoryMap.entrySet();
        for (Map.Entry<String, Class<? extends LogicalTypes.LogicalTypeFactory>> entry : logicalTypeFactoryEntries) {
            String logicalTypeName = entry.getKey();
            Class<? extends LogicalTypes.LogicalTypeFactory> logicalTypeFactoryClass = entry.getValue();
            try {
                LogicalTypes.LogicalTypeFactory logicalTypeFactory = logicalTypeFactoryClass.getDeclaredConstructor().newInstance();
                LogicalTypes.register(logicalTypeName, logicalTypeFactory);
            } catch (ReflectiveOperationException ex) {
                getLogger().error("Could not instantiate logicalTypeFactory class \"" + logicalTypeFactoryClass.getName() + "\"");
            }
        }
    }

    private void registerCustomConversions(SpecificCompiler compiler) {
        customConversions.get().forEach(compiler::addCustomConversion);
    }
}
