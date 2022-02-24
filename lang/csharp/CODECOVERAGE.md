# C# Avro Code Coverage

The following instructions should be followed in order to create a code coverage report locally.  Please note that this assumes you are running on Windows OS and that your local nuget cache is under you user profile.

1. Open a command prompt
2. Navigate to the test project `avro\lang\csharp\src\apache\test`
3. Run the following test command `dotnet test --results-directory ./TestResults --collect:"XPlat Code Coverage"`
4. Generate the report with the following command `dotnet %USERPROFILE%\.nuget\packages\reportgenerator\5.0.4\tools\net6.0\ReportGenerator.dll "-reports:./TestResults/*/coverage.cobertura.xml" "-targetdir:./Coverage/" -reporttypes:HTML`
