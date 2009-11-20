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
package org.apache.avro.tool;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.reflect.InduceSchemaTool;
import org.apache.avro.specific.SpecificCompiler.SpecificCompilerTool;

/** Command-line driver.*/
public class Main {
  /**
   * Available tools, initialized in constructor.
   */
  private final Map<String, Tool> tools;

  private Main() {
    tools = new TreeMap<String, Tool>();
    tools.put("compile", new SpecificCompilerTool());
    tools.put("induce", new InduceSchemaTool());
  }

  public static void main(String[] args) throws Exception {
    new Main().run(args);
  }

  /**
   * Delegates to tool specified on the command-line.
   */
  private void run(String[] args) throws Exception {
    if (args.length != 0) {
      Tool tool = tools.get(args[0]);
      if (tool != null) {
        tool.run(Arrays.asList(args).subList(1, args.length));
        return;
      }
    }
    System.err.println("Expected one of the following arguments:");
    for (String k : tools.keySet()) {
      System.err.println("\t" + k);
    }
  }
}
