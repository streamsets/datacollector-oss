/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import java.util.Enumeration;
import java.util.List;

public class BlackListURLClassLoader extends URLClassLoader {
  private String[] blacklistedPackages;
  private String[] blacklistedDirs;

  public BlackListURLClassLoader(List<URL> urls, ClassLoader parent, String[] blacklistedPackages) {
    super(urls.toArray(new URL[urls.size()]), parent);
    this.blacklistedPackages = (blacklistedPackages != null) ? blacklistedPackages : new String[0];
    blacklistedDirs = new String[this.blacklistedPackages.length];
    for (int i = 0; i < blacklistedDirs.length; i++) {
      blacklistedDirs[i] = this.blacklistedPackages[i].replace(".", "/");
    }
  }

  private void validateClass(String name) {
    for (String blacklistedPackage : blacklistedPackages) {
      if (name.startsWith(blacklistedPackage)) {
        throw new IllegalArgumentException(String.format("Class '%s' cannot be present in current ClassLoader", name));
      }
    }
  }

  private void validateResource(String name) {
    if (name.startsWith("/")) {
      name = name.substring(1);
    }
    for (String blacklistedPackage : blacklistedDirs) {
      if (name.startsWith(blacklistedPackage)) {
        throw new IllegalArgumentException(String.format("Resource '%s' cannot be present in current ClassLoader",
                                                         name));
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

}
