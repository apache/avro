/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.davidmc24.gradle.plugin.avro;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import org.gradle.api.file.ProjectLayout;

/**
 * General file manipulation utilities.
 *
 * <p>This copy from Apache Commons IO has been pruned down to just what is needed for gradle-avro-plugin.</p>
 *
 * <p>
 * Facilities are provided in the following areas:
 * <ul>
 * <li>writing to a file
 * <li>reading from a file
 * <li>make a directory including parent directories
 * <li>copying files and directories
 * <li>deleting files and directories
 * <li>converting to and from a URL
 * <li>listing files and directories by filter and extension
 * <li>comparing file content
 * <li>file last changed date
 * <li>calculating a checksum
 * </ul>
 * <p>
 * Origin of code: Excalibur, Alexandria, Commons-Utils
 *
 * @author <a href="mailto:burton@relativity.yi.org">Kevin A. Burton</A>
 * @author <a href="mailto:sanders@apache.org">Scott Sanders</a>
 * @author <a href="mailto:dlr@finemaltcoding.com">Daniel Rall</a>
 * @author <a href="mailto:Christoph.Reck@dlr.de">Christoph.Reck</a>
 * @author <a href="mailto:peter@apache.org">Peter Donald</a>
 * @author <a href="mailto:jefft@apache.org">Jeff Turner</a>
 * @author Matthew Hawthorne
 * @author <a href="mailto:jeremias@apache.org">Jeremias Maerki</a>
 * @author Stephen Colebourne
 * @author Ian Springer
 * @author Chris Eldredge
 * @author Jim Harrington
 * @author Niall Pemberton
 * @author Sandy McArthur
 * @version $Id: FileUtils.java 507684 2007-02-14 20:38:25Z bayard $
 */
class FileUtils {
    /**
     * Opens a {@link FileOutputStream} for the specified file, checking and
     * creating the parent directory if it does not exist.
     * <p>
     * At the end of the method either the stream will be successfully opened,
     * or an exception will have been thrown.
     * <p>
     * The parent directory will be created if it does not exist.
     * The file will be created if it does not exist.
     * An exception is thrown if the file object exists but is a directory.
     * An exception is thrown if the file exists but cannot be written to.
     * An exception is thrown if the parent directory cannot be created.
     *
     * @param file  the file to open for output, must not be <code>null</code>
     * @return a new {@link FileOutputStream} for the specified file
     * @throws IOException if the file object is a directory
     * @throws IOException if the file cannot be written to
     * @throws IOException if a parent directory needs creating but that fails
     * @since Commons IO 1.3
     */
    private static FileOutputStream openOutputStream(File file) throws IOException {
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new IOException("File '" + file + "' exists but is a directory");
            }
            if (!file.canWrite()) {
                throw new IOException("File '" + file + "' cannot be written to");
            }
        } else {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                if (!parent.mkdirs()) {
                    throw new IOException("File '" + file + "' could not be created");
                }
            }
        }
        return new FileOutputStream(file);
    }

    /**
     * Writes a String to a file creating the file if it does not exist.
     *
     * NOTE: As from v1.3, the parent directories of the file will be created
     * if they do not exist.
     *
     * @param file  the file to write
     * @param data  the content to write to the file
     * @param encoding  the encoding to use, <code>null</code> means platform default
     * @throws IOException in case of an I/O error
     * @throws java.io.UnsupportedEncodingException if the encoding is not supported by the VM
     */
    @SuppressWarnings("SameParameterValue")
    private static void writeStringToFile(File file, String data, String encoding) throws IOException {
        if (encoding == null) {
            throw new IllegalArgumentException("Must specify encoding");
        }
        try (OutputStream out = openOutputStream(file)) {
            if (data != null) {
                out.write(data.getBytes(encoding));
            }
        }
    }

    /**
     * Writes a file in a manner appropriate for a JSON file.  UTF-8 will be used, as it is the default encoding for JSON, and should be
     * maximally interoperable.
     *
     * @see <a href="https://tools.ietf.org/html/rfc7159#section-8.1">JSON Character Encoding</a>
     */
    static void writeJsonFile(File file, String data) throws IOException {
        writeStringToFile(file, data, Constants.UTF8_ENCODING);
    }

    /**
     * Acts as a replacement for {@link org.gradle.api.Project#relativePath(Object)}, as Configuration Cache support doesn't allow
     * maintaining references to the {@link org.gradle.api.Project}.
     */
    static String projectRelativePath(ProjectLayout projectLayout, File file) {
        Path path = file.toPath();
        if (path.isAbsolute()) {
            Path projectDirectoryPath = projectLayout.getProjectDirectory().getAsFile().toPath();
            path = projectDirectoryPath.relativize(path);
        } else {
            path = file.toPath();
        }
        return path.toString();
    }
}
