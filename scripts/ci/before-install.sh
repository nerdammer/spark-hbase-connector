#! /bin/bash

# Sets up the build on a travis 12.04 ubuntu

# Die on error
set -e

date

# Get some hardware details
set -x
hostname
free -m
df -h
set +x