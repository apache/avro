Periodically, we get requests for help figuring out how to
load schema files from a JAR, whether it is a JAR from a
repository or the outputs of a subproject.

`examples/avsc-from-subproject` gives an example of how
this can be configured based on a new configuration.
We might want to consider having the Avro plugin create
and configure such configurations as part of the
conventional usage pattern.

If we do this, a few gotchas:

1. We need to take into account all sourceSets (main, test, etc.)
2. We need to consider the naming so it's clear what the
   configurations are for and don't conflict with other plugins.
   For example, `additionalSchema` is too ambiguous.
   `additionalAvroSchema` might be better.
3. Whether it makes sense to exclude generated classes from jars
