#!/usr/bin/env bash

# Useful if using the same folder for windows + linux (on windows)
# since intermediate files can interfere. Sometimes you have to manually
# delete these files too (reason unknown)
find . -name "obj" -type d -exec rm -r "{}" \;
find . -name "bin" -type d -exec rm -r "{}" \;
