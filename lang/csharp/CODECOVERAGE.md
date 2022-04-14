<!--
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 -->
# C# Avro Code Coverage

The following instructions should be followed in order to create a code coverage report locally. 

1. Open a command prompt
2. Install ReportGenerator globally
	a. Run the following command line: `dotnet tool install --global dotnet-reportgenerator-globaltool --version 5.1.4 --add-source https://www.nuget.org/packages/ `
	b. The latest version can be found at [Nuget ReportGenerator](https://www.nuget.org/packages/dotnet-reportgenerator-globaltool/)
3. Navigate to the test project `avro\lang\csharp\src\apache\test`
4. Run the following test command `dotnet test --results-directory ./TestResults --collect:"XPlat Code Coverage"`
5. Generate the report with the following command `ReportGenerator "-reports:./TestResults/*/coverage.cobertura.xml" "-targetdir:./Coverage/" -reporttypes:HTML`
6. Open Report under /Coverage/index.html
