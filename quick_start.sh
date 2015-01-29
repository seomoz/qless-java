#!/bin/bash

git submodule init
git submodule update

cd qless-java/src/qless-core
make qless.lua

cd ../../../

mvn clean test
