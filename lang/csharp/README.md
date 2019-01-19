# Avro C#

## Build & Test

### Windows

1. Install [Microsoft Visual Studio Community 2017](https://www.visualstudio.com/downloads/)
2. `./build.ps1 Test`

### Linux with .NET SDK

1. Install [.NET Core SDK 2.1+](https://www.microsoft.com/net/download/linux)
2. `./build.sh test`

### Linux with Mono

1. Install [Mono v5.18+](https://www.mono-project.com/download/stable/). Install the **mono-devel**
   and **mono-complete** packages.
2. Build: `msbuild /t:"restore;build" /p:"Configuration=Release"`
3. Test: `mono ~/.nuget/packages/nunit.consolerunner/3.9.0/tools/nunit3-console.exe src/apache/test/bin/Release/net40/Avro.test.dll`

## Target Frameworks

The table below shows the frameworks that the various projects target.

Project       | Type         | .NET Standard 2.0  | .NET Core 2.0      | .NET Framework 4.0
------------  | ------------ |:------------------:|:------------------:|:------------------:
Avro.codegen  | Exe          |                    | :heavy_check_mark: | :heavy_check_mark:
Avro.ipc      | Library      |                    |                    | :heavy_check_mark:
Avro.ipc.test | Unit Tests   |                    |                    | :heavy_check_mark:
Avro.main     | Exe          | :heavy_check_mark: |                    | :heavy_check_mark:
Avro.msbuild  | Library      | :heavy_check_mark: |                    | :heavy_check_mark:
Avro.perf     | Exe          |                    | :heavy_check_mark: | :heavy_check_mark:
Avro.test     | Unit Tests   |                    | :heavy_check_mark: | :heavy_check_mark:


## Notes

The [LICENSE](./LICENSE) and [NOTICE](./NOTICE) files in the lang/csharp source directory are used to build the binary distribution. The [LICENSE.txt](../../LICENSE.txt) and [NOTICE.txt](../../NOTICE.txt) information for the Avro C# source distribution is in the root directory.
