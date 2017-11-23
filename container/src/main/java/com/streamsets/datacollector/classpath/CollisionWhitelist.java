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

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CollisionWhitelist {

  private static final Map<String, WhitelistRule> WHITELIST_RULES;
  static {
    WHITELIST_RULES = new HashMap<>();
    // Simple major version duplicate rules
    WHITELIST_RULES.put("netty", new AllowedMajorVersionsWhitelist("3", "4"));
    WHITELIST_RULES.put("jetty", new AllowedMajorVersionsWhitelist("3", "9"));
    WHITELIST_RULES.put("htrace", new AllowedMajorVersionsWhitelist("2", "3"));
    WHITELIST_RULES.put("jackson", new AllowedMajorVersionsWhitelist("1", "2"));
  }

  public static boolean isWhitelisted(
    String name,
    Map<String, List<Dependency>> dependencies
  ) {
    WhitelistRule rule = WHITELIST_RULES.get(name);

    // This dependency doesn't have special rule, hence it's not whitelisted
    if(rule == null) {
      return false;
    }

    return rule.isWhitelisted(dependencies);
  }

  /**
   * Internal interface to validate that given dependency on the various versions can be ignored.
   */
  private interface WhitelistRule {
    boolean isWhitelisted(Map<String, List<Dependency>> dependencies);
  }

  /**
   * General implementation that will allow exactly two major versions (but no duplicates in minors).
   */
  private static class AllowedMajorVersionsWhitelist implements WhitelistRule {

    private final Set<String> allowedMajors;

    public AllowedMajorVersionsWhitelist(String ... allowedMajors) {
      this.allowedMajors = new HashSet<>();
      Collections.addAll(this.allowedMajors, allowedMajors);
    }

    protected Map<String, Set<String>> distributePerMajor(Set<String> versions) {
      Map<String, Set<String>> distribution = new HashMap<>();
      versions.forEach(v -> distribution.computeIfAbsent(v.substring(0, 1), x -> new HashSet<>()).add(v));
      return distribution;
    }

    @Override
    public boolean isWhitelisted(Map<String, List<Dependency>> dependencies) {
      Map<String, Set<String>> majorDistribution = distributePerMajor(dependencies.keySet());

      // If there are more majors that those allowed in our configuration, then we know for sure that this is not valid
      if(!Sets.difference(majorDistribution.keySet(), allowedMajors).isEmpty()) {
        return false;
      }

      // Otherwise we need to make sure that each version have exactly one minor
      for(Set<String> fullVersion : majorDistribution.values()) {
        if(fullVersion.size() != 1) {
          return false;
        }
      }

      // Otherwise all is alright
      return true;
    }
  }
}
