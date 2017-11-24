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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class CollisionWhitelist {

  private static final Map<String, WhitelistRule> WHITELIST_RULES;
  static {
    WHITELIST_RULES = new HashMap<>();
    // Simple major version duplicate rules
    WHITELIST_RULES.put("jetty", new AllowedMajorVersionsWhitelist("6", "9"));
    WHITELIST_RULES.put("jersey", new AllowedMajorVersionsWhitelist("1", "2"));
    WHITELIST_RULES.put("netty", new AllowedMajorVersionsWhitelist("3", "4"));
    WHITELIST_RULES.put("metrics", new AllowedMajorVersionsWhitelist("2", "3"));
    WHITELIST_RULES.put("htrace", new AllowedMajorVersionsWhitelist("2", "3"));

    // Special rules for more complicated projects
    WHITELIST_RULES.put("jackson", new JacksonWhitelist());
  }

  /**
   * Return true if this dependency and given set of versions is whitelisted.
   *
   * This class have several rules for whitelisting - some of them are harcoded (known whitelist for all libraries),
   * whereas the optional Properties argument allows specific exceptions for this particular classpath.
   *
   * @param name Name of the dependency
   * @param specificWhitelist Properties file with exceptions
   * @param dependencies Version -> List of full dependencies
   * @return
   */
  public static boolean isWhitelisted(
    String name,
    Properties specificWhitelist,
    Map<String, List<Dependency>> dependencies
  ) {
    if(specificWhitelist != null && specificWhitelist.containsKey(name)) {
      return versionsMatch(specificWhitelist.getProperty(name), dependencies.keySet());
    }

    // Otherwise try hardcoded rules:
    WhitelistRule rule = WHITELIST_RULES.get(name);
    return rule != null && rule.isWhitelisted(dependencies);
  }

  /**
   * Compare expected versions with given versions to see if they are the same or not.
   *
   * @param expectedVersions Versions that are expected (and thus whitelisted)
   * @param versions Versions that were detected on the classpath.
   * @return True if and only if those two "sets" equals
   */
  private static boolean versionsMatch(String expectedVersions, Set<String> versions) {
    Set<String> expectedSet = Sets.newHashSet(expectedVersions.split(","));
    return Sets.symmetricDifference(expectedSet, versions).isEmpty();
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

    protected Map<String, Map<String, List<Dependency>>> distributePerMajor(Map<String, List<Dependency>> versions) {
      Map<String, Map<String, List<Dependency>>> distribution = new HashMap<>();
      versions.forEach((k, v) ->
        distribution
          .computeIfAbsent(k.substring(0, 1), x -> new HashMap<>())
          .computeIfAbsent(k, x -> new LinkedList<>())
          .addAll(v)
      );
      return distribution;
    }

    protected boolean isMajorWhitelisted(String major, Map<String, List<Dependency>> versions) {
      return versions.size() == 1;
    }

    @Override
    public boolean isWhitelisted(Map<String, List<Dependency>> dependencies) {
      Map<String, Map<String, List<Dependency>>> majorDistribution = distributePerMajor(dependencies);

      // If there are more majors that those allowed in our configuration, then we know for sure that this is not valid
      if(!Sets.difference(majorDistribution.keySet(), allowedMajors).isEmpty()) {
        return false;
      }

      // Otherwise we need to make sure that each version have exactly one minor
      for(Map.Entry<String, Map<String, List<Dependency>>> entry: majorDistribution.entrySet()) {
        if(!isMajorWhitelisted(entry.getKey(), entry.getValue())) {
          return false;
        }
      }

      // Otherwise all is alright
      return true;
    }
  }

  /**
   * Special rule for Jackson as it's kind of compound rule - it allows duplicates on major versions (1 and 2) and then
   * within 2, it's allowed to have jackson-annotations-2.8.0.jar while other jackson jars are 2.8.3.
   */
  private static class JacksonWhitelist extends AllowedMajorVersionsWhitelist implements WhitelistRule {

    public JacksonWhitelist() {
      super("1", "2");
    }

    @Override
    protected boolean isMajorWhitelisted(String major, Map<String, List<Dependency>> versions) {
      if (super.isMajorWhitelisted(major, versions)) {
        return true;
      }

      // Special detection code for major version "2"
      return major.equals("2") && validateMajor2(versions);
    }

    private boolean validateMajor2(Map<String, List<Dependency>> versions) {
      if(versions.size() != 2) {
        return false;
      }

      // Find "jackson-annotation" version prefix (2.7 , 2.8, ...)
      String versionPrefix = null;
      for(Map.Entry<String, List<Dependency>> entry : versions.entrySet()) {
        if(entry.getKey().endsWith(".0") && entry.getValue().size() == 1 && entry.getValue().get(0).getSourceName().contains("jackson-annotations")) {
          versionPrefix = entry.getKey().replaceAll("\\.[^.]*$", "");
          break;
        }
      }

      // If we haven't found the prefix, we can't whitelist
      if(versionPrefix == null) {
        return false;
      }

      // Both the versions must start with the same prefix (2.7, 2.8, ...)
      for(String version : versions.keySet()) {
        if(!version.startsWith(versionPrefix)) {
          return false;
        }
      }

      return true;
    }
  }
}
