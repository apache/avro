package com.commercehub.gradle.plugin.avro;

import org.apache.commons.io.FilenameUtils;
import org.gradle.api.specs.Spec;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class FileExtensionSpec implements Spec<File> {
    private final Set<String> extensions;

    FileExtensionSpec(String... extensions) {
        this.extensions = new HashSet<>(Arrays.asList(extensions));
    }

    FileExtensionSpec(Collection<String> extensions) {
        this.extensions = new HashSet<>(extensions);
    }

    @Override
    public boolean isSatisfiedBy(File file) {
        return extensions.contains(FilenameUtils.getExtension(file.getName()));
    }
}
