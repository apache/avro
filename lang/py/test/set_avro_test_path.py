"""
Module adjusts the path PYTHONPATH so the unittests
will work even if an egg for AVRO is already installed.
By default eggs always appear higher on pythons path then
directories set via the environment variable PYTHONPATH.

For reference see:
http://www.velocityreviews.com/forums/t716589-pythonpath-and-eggs.html
http://stackoverflow.com/questions/897792/pythons-sys-path-value.

Unittests would therefore use the installed AVRO and not the AVRO
being built. To work around this the unittests import this module before
importing AVRO. This module in turn adjusts the python path so that the test
build of AVRO is higher on the path then any installed eggs.
"""
import sys
import os

# determine the build directory and then make sure all paths that start with the
# build directory are at the top of the path
builddir=os.path.split(os.path.split(__file__)[0])[0]
bpaths=filter(lambda s:s.startswith(builddir), sys.path)

for p in bpaths:
  sys.path.insert(0,p)