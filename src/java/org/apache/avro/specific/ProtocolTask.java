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
package org.apache.avro.specific;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;

import org.apache.avro.AvroRuntimeException;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;

/** Ant task to generate Java interface and classes for a protocol. */
public class ProtocolTask extends Task {
  private File src;
  private File dest = new File(".");
  private final ArrayList<FileSet> filesets = new ArrayList<FileSet>();
  
  /** Set the schema file. */
  public void setFile(File file) { this.src = file; }
  
  /** Set the output directory */
  public void setDestdir(File dir) { this.dest = dir; }
  
  /** Add a fileset. */
  public void addFileset(FileSet set) { filesets.add(set); }
  
  /** Run the compiler. */
  public void execute() throws BuildException {
    if (src == null && filesets.size()==0)
      throw new BuildException("No file or fileset specified.");

    if (src != null)
      compile(src);

    Project myProject = getProject();
    for (int i = 0; i < filesets.size(); i++) {
      FileSet fs = filesets.get(i);
      DirectoryScanner ds = fs.getDirectoryScanner(myProject);
      File dir = fs.getDir(myProject);
      String[] srcs = ds.getIncludedFiles();
      for (int j = 0; j < srcs.length; j++) {
        compile(new File(dir, srcs[j]));
      }
    }
  }
  
  protected SpecificCompiler doCompile(File file) throws IOException {
    return SpecificCompiler.compileProtocol(file);
  }

  private void compile(File file) throws BuildException {
    try {
      SpecificCompiler compiler = doCompile(file);
      String namespace = compiler.getNamespace();
      String text = compiler.getCode();
      String name = file.getName();
      name = name.substring(0, name.indexOf('.'))+".java";
      name = SpecificCompiler.cap(name);
      File outputFile;
      if (namespace == null || namespace.length() == 0) {
        outputFile = new File(dest, name);
      } else {
        File packageDir =
            new File(dest, namespace.replace('.', File.separatorChar));
        if (!packageDir.exists()) {
            if (!packageDir.mkdirs()) {
                throw new BuildException("Unable to create " + packageDir);
            }
        }
        outputFile = new File(packageDir, name);
      }
      Writer out = new FileWriter(outputFile);
      try {
        out.write(text);
      } finally {
        out.close();
      }
    } catch (AvroRuntimeException e) {
      throw new BuildException(e);
    } catch (IOException e) {
      throw new BuildException(e);
    }
  }
}
