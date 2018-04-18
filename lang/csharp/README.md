# Avro C#

## Build & Test

### Windows

1. Install prerequisites
    - [Microsoft Visual Studio Community 2017](https://www.visualstudio.com/downloads/)
2. Open [Avro.sln](./Avro.sln) in Visual Studio
3. Build the solution
4. Run unit tests from the Test Explorer pane

### Linux

1. Install prerequisites
    - [.NET Core SDK 2.1+](https://www.microsoft.com/net/download/linux)
2. Build and run unit tests:
    ```
    cd src/apache/test
    dotnet test --framework netcoreapp2.0
    ```

## Target Frameworks

The table below shows the frameworks that the various projects target.

Project       | Type         | .NET Standard 2.0  | .NET Core 2.0      | .NET Framework 3.5 | .NET Framework 4.0
------------  | ------------ |:------------------:|:------------------:|:------------------:|:------------------:
Avro.codegen  | Exe          |                    | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark:
Avro.ipc      | Library      |                    |                    | :heavy_check_mark: | :heavy_check_mark:
Avro.ipc.test | Unit Tests   |                    |                    | :heavy_check_mark: | :heavy_check_mark:
Avro.main     | Exe          | :heavy_check_mark: |                    | :heavy_check_mark: | :heavy_check_mark:
Avro.msbuild  | Library      | :heavy_check_mark: |                    | :heavy_check_mark: | :heavy_check_mark:
Avro.perf     | Exe          |                    | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark:
Avro.test     | Unit Tests   |                    | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark:


## Notes

The [LICENSE](./LICENSE) and [NOTICE](./NOTICE) files in the lang/csharp source directory are used to build the binary distribution. The [LICENSE.txt](../../LICENSE.txt) and [NOTICE.txt](../../NOTICE.txt) information for the Avro C# source distribution is in the root directory.
