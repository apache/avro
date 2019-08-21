/**
 * Copyright © 2013 Commerce Technologies, LLC.
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

import java.io.File;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.SourceTask;

import static org.gradle.api.tasks.PathSensitivity.RELATIVE;

class OutputDirTask extends SourceTask {
    private File outputDir;

    public void setOutputDir(File outputDir) {
        this.outputDir = outputDir;
        getOutputs().dir(outputDir);
    }

    @PathSensitive(value = RELATIVE)
    public FileTree getSource() {
        return super.getSource();
    }

    @OutputDirectory
    protected File getOutputDir() {
        return outputDir;
    }

    protected FileCollection filterSources(Spec<? super File> spec) {
        return getSource().filter(spec);
    }
}
