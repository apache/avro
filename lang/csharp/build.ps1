#!/usr/bin/env pwsh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

param(
    [Parameter(Position=0)]
    [ValidateSet('Test')]
    [string]
    $Target = 'Test',

    [Parameter()]
    [ValidateSet('Release', 'Debug')]
    [string]
    $Configuration = 'Release',

    [Parameter()]
    [int]
    $BuildNumber = 0,

    [Parameter()]
    [string]
    $VersionSuffix,

    [Parameter()]
    [string]
    $RepositoryUrl
)

# Catch any PowerShell errors and exit with a non-zero exit code.
$ErrorActionPreference = 'Stop'
trap {
    $ErrorActionPreference = 'Continue'
    Write-Error $_
    exit 1
}

# Checks the LASTEXITCODE. If it is non-zero, exits with that code.
function checkExitCode {
    if ($LASTEXITCODE) {
        exit $LASTEXITCODE
    }
}

try {
    Push-Location $PSScriptRoot

    switch ($Target) {
        "Test" {
            $buildCmd = "dotnet build --configuration $Configuration -p:BuildNumber=$BuildNumber " +
                "-p:RepositoryCommit=$(git rev-parse HEAD) "

            if ($VersionSuffix) {
                $buildCmd += "-p:VersionSuffix=$VersionSuffix "
            }

            if ($RepositoryUrl) {
                $buildCmd += "-p:RepositoryUrl=$RepositoryUrl "
            }

            Invoke-Expression $buildCmd
            checkExitCode

            dotnet test --configuration $Configuration --no-build ./src/apache/test/Avro.test.csproj
            checkExitCode
        }
    }
} finally {
    Pop-Location
}
