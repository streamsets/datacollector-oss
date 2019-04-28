/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.util;

import java.util.Arrays;

/**
 * Software version class that is able to parse version string and then compare various version(s) together.
 *
 * This class will work any arbitrary number of "dots" in the version, albeit the semantic is following StreamSets
 * end of Live policy that defines the version as W.X.Y where W – Major Version, X – Minor Version, Y – Patch/BugFix.
 *
 * If incomplete version is given, this class will always assume that the missing version parts are "0". E.g. "1.1" is
 * equivalent with "1.1.0" and "1.1.0.0", ... .
 */
public class Version {

  private int []versions;

  public Version(String version) {
    // Strip away any -SNAPSHOT, -RC and other "runtime" stuff
    String[] parts = version.split("-");
    version = parts[0];

    // Version vector without any suffixes
    this.versions = Arrays.stream(version.split("\\."))
      .mapToInt(Integer::parseInt)
      .toArray();
  }

  /**
   * From W.X.Y return W.
   */
  public int getMajor() {
    return getVersionPosition(0);
  }

  /**
   * From W.X.Y return X.
   */
  public int getMinor() {
    return getVersionPosition(1);
  }

  /**
   * From W.X.Y return Y.
   */
  public int getBugfix() {
    return getVersionPosition(2);
  }

  /**
   * Return if and only if this == other.
   */
  public boolean isEqual(String other) {
    return isEqual(new Version(other));
  }

  /**
   * Return if and only if this == other.
   */
  public boolean isEqual(Version other) {
    return compare(other) == 0;
  }

  /**
   * Return if and only if this >= other.
   */
  public boolean isGreaterOrEqualTo(String other) {
    return isGreaterOrEqualTo(new Version(other));
  }

  /**
   * Return if and only if this >= other.
   */
  public boolean isGreaterOrEqualTo(Version other) {
    return compare(other) >= 0;
  }

  /**
   * Return if and only if this > other.
   */
  public boolean isGreaterThan(String other) {
    return isGreaterThan(new Version(other));
  }

  /**
   * Return if and only if this > other.
   */
  public boolean isGreaterThan(Version other) {
    return compare(other) > 0;
  }

  /**
   * Return if and only if this <= other.
   */
  public boolean isLessOrEqualTo(String other) {
    return isLessOrEqualTo(new Version(other));
  }

  /**
   * Return if and only if this <= other.
   */
  public boolean isLessOrEqualTo(Version other) {
    return compare(other) <= 0;
  }

  /**
   * Return if and only if this < other.
   */
  public boolean isLessThan(String other) {
    return isLessThan(new Version(other));
  }

  /**
   * Return if and only if this < other.
   */
  public boolean isLessThan(Version other) {
    return compare(other) < 0;
  }

  /**
   * Return version number at given position. If the position doesn't exists, return 0.
   */
  private int getVersionPosition(int index) {
    if(index < this.versions.length) {
      return versions[index];
    }

    return 0;
  }

  /**
   * Actual comparison with the same semantics as compareTo() method.
   */
  public int compare(Version other) {
    int maxParts = Math.max(this.versions.length, other.versions.length);
    for(int i = 0; i < maxParts; i++) {
      int eq = this.getVersionPosition(i) - other.getVersionPosition(i);
      if(eq != 0) {
        if(eq > 0) {
          return 1;
        } else {
          return -1;
        }
      }
    }

    return 0;
  }
}
