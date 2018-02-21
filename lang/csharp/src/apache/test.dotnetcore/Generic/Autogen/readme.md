# Avro test data

This folder contains the Avro IDL file for reference as well as the actual auto-generated .cs files. If you want to regenerate these, you'll need to get the tools yourself and create the .cs files as follows (adjust paths as needed)

## Create the intermediate avpr from the avdl

`java.exe -jar avro-tools-1.8.1.jar idl TestProtocol.avdl > TestProtocol.avpr`

## Create the .cs files from the intermediate avpr

`avrogen.exe -p TestProtocol.avpr .`

Now move the autogenerate .cs files to replace the existing ones
