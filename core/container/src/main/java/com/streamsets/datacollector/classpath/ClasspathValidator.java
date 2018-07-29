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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Classpath validator - validates that single classpath does not have conflicting dependencies. This validation is
 * best effort as it implements only known collisions and might not be smart enough to catch unknown collisions.
 */
public class ClasspathValidator {

  private static final Logger LOG = LoggerFactory.getLogger(ClasspathValidator.class);

  /**
   * Logical name for the classpath (like a stage library name, ...)
   */
  private final String name;

  /**
   * List of all dependencies for this classpath.
   */
  private final List<URL> urls = new LinkedList<>();

  // Direct instantiation is prohibited as we have a "builder" model
  private ClasspathValidator(String name) {
    this.name = name;
  }

  public static ClasspathValidator newValidator(String name) {
    return new ClasspathValidator(name);
  }

  public ClasspathValidator withURL(URL url) {
    this.urls.add(url);
    return this;
  }

  public ClasspathValidator withURLs(List<URL> urls) {
    this.urls.addAll(urls);
    return this;
  }

  public ClasspathValidator withURLs(URL ...urls) {
    this.urls.addAll(Arrays.asList(urls));
    return this;
  }

  public ClasspathValidatorResult validate() {
    return validate(null);
  }

  public ClasspathValidatorResult validate(Properties explicitWhitelist) {
    LOG.trace("Validating classpath for {}", name);
    ClasspathValidatorResult.Builder resultBuilder = new ClasspathValidatorResult.Builder(name);
    // Name of dependency -> Version(s) of the dependency -> Individual dependencies
    Map<String, Map<String, List<Dependency>>> dependecies = new HashMap<>();

    // (Re-)Parse all URLs (all dependencies)
    for(URL url : urls) {
      Optional<Dependency> parsed = DependencyParser.parseURL(url);

      if(!parsed.isPresent()) {
        resultBuilder.addUnparseablePath(url.toString());
        continue;
      }

      Dependency dependency = parsed.get();
      LOG.trace("Parsed {} to dependency {} on version {}", dependency.getSourceName(), dependency.getName(), dependency.getVersion());

      dependecies
        .computeIfAbsent(dependency.getName(), (i) -> new HashMap<>())
        .computeIfAbsent(dependency.getVersion(), (i) -> new LinkedList<>())
        .add(dependency);
    }

    // And finally validate each dependency have allowed list of versions
    for(Map.Entry<String, Map<String, List<Dependency>>> entry: dependecies.entrySet()) {
      // Ideal the inner map with versions should have only one item, otherwise that is most likely a problem
      if(entry.getValue().size() > 1) {
        if(CollisionWhitelist.isWhitelisted(entry.getKey(), explicitWhitelist, entry.getValue())) {
          LOG.trace("Whitelisted dependency {} on versions {}", entry.getKey(), StringUtils.join(entry.getValue().keySet(), ","));
          continue;
        }

        // No exceptions were applied, hence this is truly a collision
        resultBuilder.addVersionCollision(entry.getKey(), entry.getValue());
      }
    }

    return resultBuilder.build();
  }

}
