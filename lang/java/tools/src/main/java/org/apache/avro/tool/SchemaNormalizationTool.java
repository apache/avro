package org.apache.avro.tool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import joptsimple.OptionParser;

import joptsimple.OptionSet;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

public class SchemaNormalizationTool implements Tool {
  @Override
  public String getName() { return "schemacanonical"; }

  @Override
  public String getShortDescription() { return "Converts an Avro Schema to its canonical form"; }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {
    OptionParser p = new OptionParser();
    OptionSet opts = p.parse(args.toArray(new String[0]));

    if (opts.nonOptionArguments().size() != 2) {
      err.println("Expected 2 args: infile outfile (filenames or '-' for stdin/stdout)");
      p.printHelpOn(err);
      return 1;
    }

    BufferedInputStream inStream = Util.fileOrStdin(args.get(0), stdin);
    BufferedOutputStream outStream = Util.fileOrStdout(args.get(1), out);

    Schema schema = new Schema.Parser().parse(inStream);

    String canonicalForm = SchemaNormalization.toParsingForm(schema);

    outStream.write(canonicalForm.getBytes());

    Util.close(inStream);
    Util.close(outStream);

    return 0;
  }
}
