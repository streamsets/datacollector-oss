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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Contains results from classpath validation.
 */
public class ClasspathValidatorResult {

  private static final Logger LOG = LoggerFactory.getLogger(ClasspathValidatorResult.class);

  /**
   * Logical name for the classpath (like a stage library name, ...)
   */
  private final String name;
  public String getName() {
    return name;
  }

  /**
   * Set of paths that failed to parse during validation.
   */
  final private Set<String> unparseablePaths;
  public Set<String> getUnparseablePaths() {
    return unparseablePaths;
  }

  /**
   * Version collisions.
   *
   * The structure is Dependency name -> Version -> Dependency structure (paths and such).
   */
  final private Map<String, Map<String, List<Dependency>>> versionCollisions;
  public Map<String, Map<String, List<Dependency>>> getVersionCollisions() {
    return versionCollisions;
  }

  /**
   * Return true if and only if the validation was successful and no problems were found.
   */
  public boolean isValid() {
    return unparseablePaths.isEmpty() && versionCollisions.isEmpty();
  }

  /**
   * Generate small report into log.
   */
  public void logDetails() {
    if(isValid()) {
      return;
    }

    LOG.warn("Validation results for {}", name);

    if(!unparseablePaths.isEmpty()) {
      LOG.warn("Can't parse the following artifacts:");
      for(String path : unparseablePaths) {
        LOG.warn("  {}", path);
      }
    }

    if(!versionCollisions.isEmpty()) {
      LOG.warn("Detected colliding dependency versions:");
      for(Map.Entry<String, Map<String, List<Dependency>>> entry : versionCollisions.entrySet()) {
        LOG.warn("  Dependency {} have versions: {}", entry.getKey(), StringUtils.join(entry.getValue().keySet(), ", "));
        for(Map.Entry<String, List<Dependency>> versionEntry : entry.getValue().entrySet()) {
          LOG.warn("    Version: {}", versionEntry.getKey());
          for(Dependency dependency: versionEntry.getValue()) {
            LOG.warn("      {}", dependency.getSourceName());
          }
        }
      }
    }
  }

  private ClasspathValidatorResult(
    String name,
    Set<String> unparseablePaths,
    Map<String, Map<String, List<Dependency>>> versionCollisions
  ) {
    this.name = name;
    this.unparseablePaths = Collections.unmodifiableSet(unparseablePaths);
    this.versionCollisions = Collections.unmodifiableMap(versionCollisions);
  }

  public static class Builder {
    final private String name;
    final private Set<String> unparseablePaths = new HashSet<>();
    final private Map<String, Map<String, List<Dependency>>> versionCollisons = new HashMap<>();

    public Builder(String name) {
      this.name = name;
    }

    public Builder addUnparseablePath(String path) {
      this.unparseablePaths.add(path);
      return this;
    }

    public Builder addVersionCollision(String dependency, Map<String, List<Dependency>> collision) {
      this.versionCollisons.put(dependency, collision);
      return this;
    }

    public ClasspathValidatorResult build() {
      return new ClasspathValidatorResult(
        name,
        unparseablePaths,
        versionCollisons
      );
    }
  }
}
