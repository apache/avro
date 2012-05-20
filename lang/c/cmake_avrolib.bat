echo off

REM Set up the solution file in Windows. 

set my_cmake_path="put_your_cmake_path_here"
set cmake_path_win7="C:\Program Files (x86)\CMake 2.8\bin\cmake.exe"
set cmake_path_xp="C:\Program Files\CMake 2.8\bin\cmake.exe"

if exist %my_cmake_path% (
   set cmake_path=%my_cmake_path%
   goto RUN_CMAKE
)

if exist %cmake_path_win7% (
   set cmake_path=%cmake_path_win7%
   goto RUN_CMAKE
)

if exist %cmake_path_xp% (
   set cmake_path=%cmake_path_xp%
   goto RUN_CMAKE
)

echo "Set the proper cmake path in the variable 'my_cmake_path' in cmake_windows.bat, and re-run"
goto EXIT_ERROR

:RUN_CMAKE
%cmake_path% -G"Visual Studio 9 2008" -H. -Bbuild_win32


:EXIT_ERROR
