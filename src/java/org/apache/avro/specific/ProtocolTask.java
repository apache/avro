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

import java.io.*;
import java.util.ArrayList;
import org.apache.tools.ant.*;
import org.apache.tools.ant.types.FileSet;

import org.apache.avro.*;

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
  
  protected String doCompile(File file) throws IOException {
    return SpecificCompiler.compileProtocol(file);
  }

  private void compile(File file) throws BuildException {
    try {
      String text = doCompile(file);
      String name = file.getName();
      name = name.substring(0, name.indexOf('.'))+".java";
      name = SpecificCompiler.cap(name);
      Writer out = new FileWriter(new File(dest, name));
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
