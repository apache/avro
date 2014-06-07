package com.commercehub.gradle.plugin.avro;

import org.gradle.api.Project;

@Deprecated
public class UnqualifiedAvroPlugin extends AvroPlugin {
    // TODO: remove this class (and related avro.properties) in 0.4.0.

    @Override
    public void apply(Project project) {
        project.getLogger().warn("The 'avro' plugin ID is deprecated; " +
                "please update your build to use 'com.commercehub.gradle.plugin.avro' instead.");
        super.apply(project);
    }
}
