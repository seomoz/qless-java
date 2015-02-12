#!/bin/bash

git submodule update --init --recursive

make -C src/qless-core

mvn clean test
