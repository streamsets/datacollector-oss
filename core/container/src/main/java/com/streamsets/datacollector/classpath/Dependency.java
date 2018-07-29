/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.classpath;

/**
 * Describes Java dependency.
 *
 * This class is meant to be used at runtime and hence the name is represented by simple name rather then maven ids.
 */
public class Dependency {

  /**
   * Name of the source jar file that was parsed.
   */
  private String sourceName;

  /**
   * Dependency name.
   *
   * Usually whatever is in the jar name before version.
   */
  private String name;

  /**
   * Dependency version.
   *
   * Whatever is encoded in the jar file name.
   */
  private String version;

  public Dependency(String name, String version) {
    this(null, name, version);
  }

  public Dependency(String sourceName, String name, String version) {
    this.sourceName = sourceName;
    this.name = name;
    this.version = version;
  }

  public String getSourceName() {
    return sourceName;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }
}
