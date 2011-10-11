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
package org.apache.avro.compiler.specific;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericData.StringType;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;

/** Ant task to generate Java interface and classes for a protocol. */
public class ProtocolTask extends Task {
  private File src;
  private File dest = new File(".");
  private StringType stringType = StringType.CharSequence;

  private final ArrayList<FileSet> filesets = new ArrayList<FileSet>();
  
  /** Set the schema file. */
  public void setFile(File file) { this.src = file; }
  
  /** Set the output directory */
  public void setDestdir(File dir) { this.dest = dir; }
  
  /** Set the string type. */
  public void setStringType(StringType type) { this.stringType = type; }
  
  /** Get the string type. */
  public StringType getStringType() { return this.stringType; }
  
  /** Add a fileset. */
  public void addFileset(FileSet set) { filesets.add(set); }
  
  /** Run the compiler. */
  @Override
  public void execute() {
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
  
  protected void doCompile(File src, File dir) throws IOException {
    Protocol protocol = Protocol.parse(src);
    SpecificCompiler compiler = new SpecificCompiler(protocol);
    compiler.setStringType(getStringType());
    compiler.compileToDestination(src, dest);
  }

  private void compile(File file) {
    try {
      doCompile(file, dest);
    } catch (AvroRuntimeException e) {
      throw new BuildException(e);
    } catch (IOException e) {
      throw new BuildException(e);
    }
  }
}

