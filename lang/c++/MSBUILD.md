# Visual Studio 2019 Build Instructions

## Prerequisites

 * Microsoft Visual Studio 2019.
 * CMake >= 3.12 (should be supplied as part of VS2019 installation).
 * Clone [https://github.com/spektom/snappy-visual-cpp](https://github.com/spektom/snappy-visual-cpp), and follow build instructions in `README.md`.
 * Install Boost from [https://netcologne.dl.sourceforge.net/project/boost/boost-binaries/1.68.0/boost_1_68_0-msvc-14.1-64.exe](https://netcologne.dl.sourceforge.net/project/boost/boost-binaries/1.68.0/boost_1_68_0-msvc-14.1-64.exe).
 * Add `C:\<path to>\boost_1_68_0\lib64-msvc-14.1` to PATH environment variable.

## Building

    cd lang\c++
    cmake -G "Visual Studio 16 2019" -DBOOST_ROOT=C:\<path to>\boost_1_68_0 -DBOOST_INCLUDEDIR=c:\<path to>\boost_1_68_0\boost  -DBOOST_LIBRARYDIR=c:\<path to>\boost_1_68_0\lib64-msvc-14.1 -DSNAPPY_INCLUDE_DIR=C:\<path to>\snappy-visual-cpp -DSNAPPY_LIBRARIES=C:\<path to>\snappy-visual-cpp\x64\Release\snappy.lib ..
    msbuild Avro-cpp.sln /p:Configuration=Release /p:Platform=x64

