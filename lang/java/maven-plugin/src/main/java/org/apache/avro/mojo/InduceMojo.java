/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.mojo;

import java.io.File;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.apache.avro.reflect.ReflectData;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

/**
 * Generate Avro files (.avsc and .avpr) from Java classes or interfaces
 *
 * @goal induce
 * @phase process-classes
 * @threadSafe
 */
public class InduceMojo extends AbstractMojo {
  /**
   * The source directory of Java classes.
   *
   * @parameter property="sourceDirectory"
   *            default-value="${basedir}/src/main/java"
   */
  private File sourceDirectory;

  /**
   * Directory where to output Avro schemas (.avsc) or protocols (.avpr).
   *
   * @parameter property="outputDirectory"
   *            default-value="${basedir}/generated-resources/avro"
   */
  private File outputDirectory;

  /**
   * The current Maven project.
   *
   * @parameter default-value="${project}"
   * @readonly
   * @required
   */
  protected MavenProject project;

  public void execute() throws MojoExecutionException {
    ClassLoader classLoader = getClassLoader();

    for (File inputFile : sourceDirectory.listFiles()) {
      String className = parseClassName(inputFile.getPath());
      Class<?> klass = loadClass(classLoader, className);
      String fileName = outputDirectory.getPath() + "/" + parseFileName(klass);
      File outputFile = new File(fileName);
      outputFile.getParentFile().mkdirs();
      try {
        PrintWriter writer = new PrintWriter(fileName, "UTF-8");
        if (klass.isInterface()) {
          writer.println(ReflectData.get().getProtocol(klass).toString(true));
        } else {
          writer.println(ReflectData.get().getSchema(klass).toString(true));
        }
        writer.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private String parseClassName(String fileName) {
    String indentifier = "java/";
    int index = fileName.lastIndexOf(indentifier);
    String namespacedFileName = fileName.substring(index + indentifier.length());

    return namespacedFileName.replace("/", ".").replace(".java", "");
  }

  private String parseFileName(Class klass) {
    String className = klass.getName().replace(".", "/");
    if (klass.isInterface()) {
      return className.concat(".avpr");
    } else {
      return className.concat(".avsc");
    }
  }

  private Class<?> loadClass(ClassLoader classLoader, String className) {
    Class<?> klass = null;

    try {
      klass = classLoader.loadClass(className);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    return klass;
  }

  private ClassLoader getClassLoader() {
    ClassLoader classLoader = null;

    try {
      List<String> classpathElements = project.getRuntimeClasspathElements();
      if (null == classpathElements) {
        return Thread.currentThread().getContextClassLoader();
      }
      URL[] urls = new URL[classpathElements.size()];

      for (int i = 0; i < classpathElements.size(); ++i) {
        urls[i] = new File(classpathElements.get(i)).toURI().toURL();
      }
      classLoader = new URLClassLoader(urls, getClass().getClassLoader());
    } catch (Exception e) {
      e.printStackTrace();
    }

    return classLoader;
  }
}
