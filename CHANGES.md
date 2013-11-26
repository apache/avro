# Change Log

* 0.1.3
    * Always regenerate all Java classes when any schema file changes to avoid some classes having outdated schema information.

* 0.1.2
    * Eliminate dependency on guava, make dependency on commons-io explicit

* 0.1.1
    * Fixed NullPointerException when performing clean builds

* 0.1.0
    * Add support for converting IDL files to JSON protocol declaration files
    * Add support for generating Java classes from JSON protocol declaration files
    * Add support for generating Java classes from JSON schema declaration files
    * Add support for inter-dependent JSON schema declaration files
    * Add support for tweaking source/exclude directories in IntelliJ
    * Add support for specifying the string type to use in generated classes
