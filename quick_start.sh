#!/bin/bash

git submodule update --init --recursive

make -C qless-java/src/qless-core

mvn clean test
