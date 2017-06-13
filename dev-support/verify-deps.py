#!/usr/bin/env python
#
# Copyright 2017 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script for ensuring that we're not mistakenly including one dependency twice inside
# the same stage library - e.g. that we're not for example shipping two different guava
# versions inside the same stage lib and thus the same classloader.
import argparse
import logging
import os
import re
import sys

# The core functionality of this script - regular expression that can retrieve dependency name and version from given
# jar's file name. We have one generic expression for "most" usual cases and then a second tuple for "special" cases.
# The expected format is $dependencyName-$version-$someCommonSuffixes.jar
ALLOWED_SUFFIXES=("-tests", "-GA", "-incubating", "-native", "\.Final", "-shaded-protobuf", "\.RELEASE", "-jhyde", "-shaded", "-m3", "-indy", "-hadoop2")
BASIC_NAME = re.compile("^([A-Za-z_.0-9-]+)-([0-9aMbvr.]+)(" + '|'.join(ALLOWED_SUFFIXES) + ")?\.jar$")

# Special cases.
#
# * Key is regular expression that will be applied on the file name
# * Value is a lambda function that will be called if key matched. Expected return value is tuple ($dependencyName, $dependencyVersion)
COMPLEX_CASES = (
  # Our own dependencies - we're squashing all various jars together here as we want all of our jars on single version
  (re.compile("^streamsets-(.*)-(([0-9.]+)(-SNAPSHOT))?\.jar$"), lambda m: ('streamsets', m.group(2))),
  # Squashing multiple different jars to single dependency
  (re.compile("^avro-(.*)-([0-9.]+)(-tests|-hadoop2)?\.jar$"), lambda m: ('avro', m.group(2))),
  (re.compile("^jackson-annotations-2.5.0.jar$"), lambda m: ('jackson', '2.5.4')),
  (re.compile("^jackson-(.*)-([0-9.]+)\.jar$"), lambda m: ('jackson', m.group(2)) ),
  (re.compile("^netty-(.*-)?([0-9.]+)\.Final(-linux-x86_64)?\.jar$"), lambda m: ('netty', m.group(2))),
  # Cloudera's specialities
  (re.compile("^([A-Za-z_.0-9-]+)-(([0-9.]+)-cdh([0-9.]+))\.jar$"), lambda m: (m.group(1), m.group(2))),
  (re.compile("^([A-Za-z_.0-9-]+)-(([0-9.]+)\.cloudera\.4)\.jar$"), lambda m: (m.group(1), m.group(2))),
  # Hortonwork's specialities
  (re.compile("^([A-Za-z_.0-9-]+)-(([0-9.]+)\.hwx)\.jar$"), lambda m: (m.group(1), m.group(2))),
  # Various special weirdness
  (re.compile("^jython\.jar$"), lambda m: ('jython', 'unspecified')),

)

# Allowed exceptions - stage libraries for which we do ship multiple versions either because we know that it's
# safe or because it's the vendor who by themselves ships multiple versions of the same library.
EXCEPTIONS = {
  "streamsets-datacollector-cdh_5_4-lib": {
    "htrace-core": set(["3.0.4", "3.1.0"]),
  },
  "streamsets-datacollector-cdh_5_5-lib": {
    "metrics-json": set(["3.1.2", "3.0.2"]),
    "metrics-jvm":  set(["3.1.2", "3.0.2"]),
  },
  "streamsets-datacollector-cdh_5_6-lib": {
    "metrics-json": set(["3.1.2", "3.0.2"]),
    "metrics-jvm":  set(["3.1.2", "3.0.2"]),
  },
  "streamsets-datacollector-cdh_5_7-lib": {
    "metrics-json": set(["3.1.2", "3.0.2"]),
    "metrics-jvm":  set(["3.1.2", "3.0.2"]),
  },
  "streamsets-datacollector-cdh_5_8-lib": {
    "metrics-json": set(["3.1.2", "3.0.2"]),
    "metrics-jvm":  set(["3.1.2", "3.0.2"]),
  },
  "streamsets-datacollector-cdh_5_9-lib": {
    "metrics-json": set(["3.1.2", "3.0.2"]),
    "metrics-jvm":  set(["3.1.2", "3.0.2"]),
  },
}

# Regular expression for file names that should be skipped
#
# * From some reason some stages are pulling pom files
# * Spark is correctly shading protobuf jar, but is creating file that is not implying that it's spark related :(
SKIP = re.compile("^protobuf-java-2.4.1-shaded.jar|.*\.pom$")

# Parse given file name into tuple (name, version)
#
# This method will raise an exception if the filename can't be matched.
def parse_dep_name(name):
  # Go over complex cases first to clean them up
  for r, f in COMPLEX_CASES:
    m = re.match(r, name)
    if m:
      return f(m)

  # Last resort - simple name
  m = re.match(BASIC_NAME, name)
  if m:
    return (m.group(1), m.group(2))

  # Nothing worked, throw an exception
  raise ValueError('Can not parse dependency name: {0}'.format(name))

# Validate if given versions list have exactly two allowed entries
#
# For things like netty (3 & 4) or jackson (1 & 2)
def is_duplicate_dual(versions, first, second):
  # Ignore single value
  if len(versions) == 1:
      return False

  # We're validating only pairs, so anything more is a problem
  if len(versions) > 2:
     return True

  one = False
  two = False
  for version in versions:
    if(version.startswith(first)):
      one = True
    if(version.startswith(second)):
      two = True
  return not(one and two)

# Verify whether given versions contains duplicates
#
# We have special function for this as some duplicates are "allowed" (for some definition of the word "allowed")
def is_duplicate(name, versions):
  if name == "jackson":
    return is_duplicate_dual(versions, "1", "2")
  if name == "netty":
    return is_duplicate_dual(versions, "3", "4")
  if name == "jetty-util":
    return is_duplicate_dual(versions, "6", "9")
  if name == "htrace-core":
    return is_duplicate_dual(versions, "2", "3")

  # In default state, having more then one version is considered bad
  return len(versions) > 1

# Try to get stage lib name from given path
#
# Returns either stage lib name or None if the path can't be parsed
def stage_lib_from_path(path):
  m = re.match(re.compile("^.*/(streamsets-datacollector-[a-zA-Z0-9_-]+)/lib/?$"), path)
  return m.group(1) if m else None

# Verify dependencies given directory
#
# Returns dictionary with errors, key is dependency name, value is list of versions found
def verify_dir(path):
  logging.debug("Processing path %s", path)

  # Build dependency map
  dependencies = {}
  for file in os.listdir(path):
    # Skip files that doesn't make sense to work with
    if re.match(SKIP, file):
      continue

    # Parse filename and build internal map of all versions for given dependency
    name, version = parse_dep_name(file)
    logging.debug("Processing dependency file %s: dependency=%s, version=%s", file, name, version)
    if not name in dependencies:
      dependencies[name] = set()
    dependencies[name].add(version)

  # Filter dependencies with multiple versions
  return {name: versions
          for name, versions in dependencies.items()
          if is_duplicate(name, versions)}

# Filter "allowed" duplicates out
def filter_allowed(stage_lib, errors):
  return {name: version
          for name, version in errors.items()
          if not(stage_lib in EXCEPTIONS and name in EXCEPTIONS[stage_lib] and set(version) == EXCEPTIONS[stage_lib][name])}

# User printable result of a scan for given directory
def print_errors(directory, errors):
  stage_lib = stage_lib_from_path(directory) or director

  if len(errors) == 0:
    logging.info("Stage lib %s doesn't have any duplicate dependencies.", stage_lib)
  else:
    filtered = filter_allowed(stage_lib, errors)

    logging.info("Stage lib %s have duplicate dependencies (total of %s from which %s are white listed)", stage_lib, len(errors), len(errors) -  len(filtered))
    for name, versions in filtered.items():
      logging.info("\t Dependency %s have versions %s", name, ', '.join(versions))

# Run the script for single directory
def run_directory(directory):
  print_errors(directory, verify_dir(directory))

# Run the script for whole release
def run_release(directory):
  logging.info("Running on whole release at %s", directory)

  for stage_lib in os.listdir(os.path.join(directory, "streamsets-libs")):
    path = os.path.join(directory, "streamsets-libs", stage_lib, "lib")
    print_errors(path, verify_dir(path))


# Main execution
parser = argparse.ArgumentParser(description='Validate stage library or entire SDC release to make sure that stage libs doesn\'t have multiple versions of a single dependency.')
parser.add_argument('--stage-lib',  action='store_true',    required=False, help='Action: validate given directory as a stage lib')
parser.add_argument('--release',    action='store_true',    required=False, help='Action: validate release directory (all stage libs)')
parser.add_argument('--directory',  action='store',         required=True,  help='Directory that should be verified.')
parser.add_argument('--verbose',    action='store_true',    required=False, help='Print out much more information')
args = parser.parse_args()

# Validate args
if args.stage_lib and args.release:
  raise SyntaxError("--stage-lib and --release can't be used together")
if not args.directory:
  raise SyntaxError("--directory is missing")

# Logging configuration
if args.verbose:
  logging.basicConfig(level=logging.DEBUG)
else:
  logging.basicConfig(level=logging.INFO)

# Run the script
if args.stage_lib:
  run_directory(args.directory)
elif args.release:
  run_release(args.directory)
else:
  raise SyntaxError("I'm not sure what to do, please pick the phone and call Arvind P. to figure that out.")
