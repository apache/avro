## test for parsing multiple files.
This folder aims to test `public List<Schema> Schema.parse(Iterable<File> sources) throws IOException` method.

The objective is to check that a record schema define in a file can be use in another record schema as a field type.
Here, ApplicationEvent.avsc file contains a field of type DocumentInfo, defined in file DocumentInfo.avsc.

The is written at TestSchema.testParseMultipleFile.

