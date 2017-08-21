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
package com.streamsets.pipeline;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class BlackListURLClassLoader extends URLClassLoader {
  private final String type;
  private final String name;
  private final String[] blacklistedPackages;
  private final String[] blacklistedDirs;

  protected final List<URL> urls;


  public BlackListURLClassLoader(String type, String name, List<URL> urls, ClassLoader parent,
                                 String[] blacklistedPackages) {
    super(urls.toArray(new URL[urls.size()]), parent);
    this.urls = urls;
    this.type = type;
    this.name = name;
    this.blacklistedPackages = (blacklistedPackages != null) ? blacklistedPackages : new String[0];
    blacklistedDirs = new String[this.blacklistedPackages.length];
    for (int i = 0; i < blacklistedDirs.length; i++) {
      blacklistedDirs[i] = this.blacklistedPackages[i].replace(".", "/");
    }
  }

  // Visible for testing only
  void validateClass(String name) {
    // python scripting engine generates __*__ classes under all packages (including SDC api package)
    if (!(name.endsWith("__"))) {
      for (String blacklistedPackage : blacklistedPackages) {
        if (name.startsWith(blacklistedPackage)) {
          throw new IllegalArgumentException(String.format("Class '%s' cannot be present in %s",
            name, toString()));
        }
      }
    }
  }

  // Visible for testing only
  void validateResource(String name) {
    for (String blacklistedPackage : blacklistedDirs) {
      if (name.startsWith(blacklistedPackage)) {
        throw new IllegalArgumentException(String.format("Resource '%s' cannot be present in %s",
                                                         name, toString()));
      }
    }
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    validateClass(name);
    return super.findClass(name);
  }

  @Override
  public URL findResource(String name) {
    validateResource(name);
    return super.findResource(name);
  }

  @Override
  public Enumeration<URL> findResources(String name) throws IOException {
    validateResource(name);
    return super.findResources(name);
  }

  public String toString() {
    return String.format("BlackListURLClassLoader[type=%s name=%s]", type, name);
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String dumpClasspath(String linePrefix) {
    List<String> entries = new ArrayList<>();
    dumpClasspath(this, entries);
    StringBuilder sb = new StringBuilder();
    sb.append(linePrefix).append("ClassLoader name=").append(getName()).append(" type=").append(getType()).append("\n");
    for (String entry : entries) {
      sb.append(linePrefix).append("  ").append(entry).append("\n");
    }
    return sb.toString();
  }

  private String dumpClasspath(ClassLoader classLoader, List<String> entries) {
    String prefix = "";
    if (classLoader.getParent() != null && classLoader.getParent() instanceof URLClassLoader) {
      prefix += "  " + dumpClasspath(classLoader.getParent(), entries);
    }
    if (classLoader instanceof URLClassLoader) {
      for (URL url : ((URLClassLoader) classLoader).getURLs()) {
        entries.add(prefix + url.toString());
      }
    }
    return prefix;
  }

}
