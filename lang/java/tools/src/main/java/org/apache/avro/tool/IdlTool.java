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


import org.apache.avro.Protocol;
import org.apache.avro.compiler.idl.Idl;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tool implementation for generating Avro JSON schemata from
 * idl format files.
 */
public class IdlTool implements Tool {

    private final Option help = Option.builder("h").longOpt("help")
            .desc("Display help information").build();

    private final Option include = Option.builder("i").longOpt("include")
            .hasArg(true).argName("include_dir")
            .desc("Includes the specified include dir for import resolvement")
            .build();

    private final Options options = new Options()
            .addOption(help)
            .addOption(include);

    private final CommandLineParser parser = new DefaultParser();
    private final HelpFormatter formatter = new HelpFormatter();

    @Override
    public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
        CommandLine line = parser.parse(options, args.toArray(new String[]{}));

        if (line.hasOption(help.getOpt())) {
            printHelp();
            return 0;
        }

        String fileIn = "-";
        String fileOut = "-";
        List<File> includeDirs = new ArrayList<File>();

        if (line.hasOption(include.getOpt())) {
            for (String inclDir : line.getOptionValues(include.getOpt())) {
                includeDirs.add(new File(inclDir));
            }
        }
        if (line.getArgs().length >= 1) {
            fileIn = line.getArgs()[0];
        }
        if (line.getArgs().length >= 2) {
            fileOut = line.getArgs()[1];
        }

        //Check arguments
        int rc = 0;
        for (File dir : includeDirs) {
            if (!dir.exists() || !dir.isDirectory()) {
                System.err.println("");
                System.err.println("Error: Specified " + include.getArgName() + " path '" + dir + "' does not exists or is not a directory!");
                System.err.println("");
                rc = -1;
            }
        }
        if (!"-".equals(fileIn)) {
            File f = new File(fileIn);
            if (!f.exists() || f.isDirectory()) {
                System.err.println("");
                System.err.println("Error: Specified input file '" + fileIn + "' does not exists or is a directory!");
                System.err.println("");
                rc = -1;
            }
        }
        if (!"-".equals(fileOut)) {
            File f = new File(fileOut);
            if (!f.exists() || f.isDirectory()) {
                System.err.println("");
                System.err.println("Error: Specified output file '" + fileOut + "' does not exists or is a directory!");
                System.err.println("");
                rc = -1;
            }
        }

        if (rc != 0) {
            printHelp();
        } else {
            Idl parser = null;
            if ("-".equals(fileIn)) {
                parser = new Idl(in);
            } else {
                parser = new Idl(new File(fileIn));
            }

            PrintStream parseOut = out;
            if (!"-".equals(fileOut)) {
                parseOut = new PrintStream(new FileOutputStream(fileOut));
            }

            for (File includeDir : includeDirs) {
                parser.addIncludeDir(includeDir);
            }

            Protocol p = parser.CompilationUnit();
            parseOut.print(p.toString(true));
        }

        return rc;
    }

    private void printHelp() {
        StringBuffer header = new StringBuffer();
        header.append("If an output path is not specified, outputs to stdout.");
        header.append(System.getProperty("line.separator"));
        header.append("If no input or output is specified, takes input from stdin and outputs to stdout.");
        header.append(System.getProperty("line.separator"));
        header.append("The special path \"-\" may also be specified to refer to stdin and stdout.");
        header.append(System.getProperty("line.separator"));
        header.append(System.getProperty("line.separator"));

        formatter.printHelp(
                "idl [--include <include> ...] [in] [out]",
                header.toString(), options, null);
    }

    @Override
    public String getName() {
        return "idl";
    }

    @Override
    public String getShortDescription() {
        return "Generates a JSON schema from an Avro IDL file";
    }
}
