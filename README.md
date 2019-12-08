# Apache Avro™

Apache Avro™ is a data serialization system.

Learn more about Avro, please visit our website at:

  https://avro.apache.org/

# About this version 
- support multiple schemas files
- allow reference to types on external schema file

## To test it:

### Build project Avro.codegen. 
This project generate the dotnet tool, it was renaming as *avrogenms* (instead of avrogen) to avoid conflict wth original Apache tool.
When buil, a nuget package with *avrogenms* tool will be generated.
Package version is read from file "share\VERSION.txt", currently set to "1.10.1-ms"

### Install avrogenms tool
You can install this tool globally or locally, I prefere to do it locally. 

To install the tool:
- open command prompt on "Avro.sampleMultiSchema" folder (you can do it on any folder but just keep it as root folder for all other steps)
- run command: 

Local install (on folder "Avro.sampleMultiSchema\tool"):

`> dotnet tool install --tool-path .\tool --add-source ..\..\..\..\lang\csharp\src\apache\codegen\bin\Debug\ Apache.Avro.Tools --version 1.10.1-ms
`

Global install:

`> dotnet tool install --global --add-source ..\..\..\..\lang\csharp\src\apache\codegen\bin\Debug\ Apache.Avro.Tools --version 1.10.1-ms
`

you should have message as:

`
You can invoke the tool using the following command: avrogenms.
Tool 'apache.avro.tools' (version '1.10.1-ms') was successfully installed.
`

The project Avro.sampleMultiSchema contains 2 schema files that are used to test. To generate C# classes from these files just build project Avro.sampleMultiSchema. Or run the tool from command line as follow:

`
Avro.sampleMultiSchema>.\tool\avrogenms -ms .\avroFiles\schema01.avsc;.\avroFiles\schema02.avsc .\generated
`

If you build the project, the build should pass including the 2 generated C# classes from schema files.
If you exectue the tool from command line, the 2 c# classes are generated at folder "Avro.sampleMultiSchema\generated"

*schema01.avsc* generates class *generated\org\vehicule.cs* which contains the field *model* of type *CarModel* which is declared on class *generated\com\cars\CarModel.cs* generated from *schema.02.avsc*.
