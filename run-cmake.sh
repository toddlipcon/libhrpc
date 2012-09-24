# Copyright (c) 2013, Cloudera, inc.
CMAKE_PREFIX_PATH=/opt CC=clang CXX=clang++ \
  cmake . -DCMAKE_USER_MAKE_RULES_OVERRIDE=ClangOverride.txt -DCMAKE_BUILD_TYPE=Debug
