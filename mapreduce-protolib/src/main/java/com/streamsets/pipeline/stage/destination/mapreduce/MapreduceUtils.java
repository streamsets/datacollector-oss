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
package com.streamsets.pipeline.stage.destination.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public final class MapreduceUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MapreduceUtils.class);

  /**
   * See {@link org.apache.hadoop.util.GenericOptionsParser}
   */
  public static final String TMP_JARS_PROPERTY = "tmpjars";
  public static final String JAR_SEPARATOR = ",";

  /**
   * Returns path to the jar containing location to given class.
   */
  public static String jarForClass(Class klass) {
    return klass.getProtectionDomain().getCodeSource().getLocation().toString();
  }

  /**
   * Add jars containing the following classes to the job's classpath.
   */
  public static void addJarsToJob(Configuration conf, Class ...klasses) {
    // Build set of jars that needs to be added, order doesn't matter for us and we will remove duplicates
    Set<String> additinonalJars = new HashSet<>();
    for(Class klass : klasses) {
      final String jar = jarForClass(klass);
      LOG.info("Adding jar {} for class {}", jar, klass.getCanonicalName());
      additinonalJars.add(jar);
    }

    appendJars(conf, additinonalJars);
  }

  /**
   * Add jars whose names contain the given patterns to the job's classpath.
   *
   * Searches the {@link ClassLoader} - which must be an instance of {@link URLClassLoader} or an
   * {@link IllegalStateException} will be thrown - of the {@link MapreduceUtils} class itself for the jar matching
   * a pattern indicated each arguments.
   *
   * Each supplied jarPattern is treated as a case-insensitive substring for a full jar path.  This method expects
   * to find at least one jar matching each substring, or an {@link IllegalArgumentException} will be thrown.  If more
   * than one jar is matched, then the result will depend on the allowMultiple parameter - if true, then all will be
   * added, while if false, an {@link IllegalArgumentException} will be thrown.
   *
   * Care has <b>NOT</b> been taken to optimize the performance of this method; the runtime is M * N where M is the
   * number of entries on the classpath and N is the number of given jarPatterns.  It is assumed that this method
   * will be called infrequently and with relatively few jarPatterns.
   *
   * @param conf the Hadoop conf for the MapReduce job
   * @param allowMultiple whether multiple matching jars are allowed to be added for a single pattern
   * @param jarPatterns the patterns to search for within the classpath
   */
  public static void addJarsToJob(Configuration conf, boolean allowMultiple, String... jarPatterns) {
    final ClassLoader loader = MapreduceUtils.class.getClassLoader();
    if (!(loader instanceof URLClassLoader)) {
      throw new IllegalStateException(String.format(
          "ClassLoader for %s is not an instance of URLClassLoader (it is %s), and thus this method cannot be used",
          MapreduceUtils.class.getCanonicalName(),
          loader.getClass().getCanonicalName()
      ));
    }
    final URLClassLoader urlClassLoader = (URLClassLoader) loader;

    addJarsToJob(conf, allowMultiple, urlClassLoader.getURLs(), jarPatterns);
  }

  @VisibleForTesting
  static void addJarsToJob(Configuration conf, boolean allowMultiple, URL[] jarUrls, String... jarPatterns) {

    final List<String> allMatches = new LinkedList<>();
    final List<URL> patternMatches = new LinkedList<>();

    for (String pattern : jarPatterns) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Looking for pattern {}", pattern);
      }
      for (URL url : jarUrls) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Looking in jar {}", url);
        }
        final String file = url.getFile();
        if (StringUtils.endsWithIgnoreCase(file, ".jar") && StringUtils.containsIgnoreCase(file, pattern)) {
          allMatches.add(url.toString());
          patternMatches.add(url);

          if (LOG.isDebugEnabled()) {
            LOG.debug("Found jar {} for pattern {}", url, pattern);
          }
        }
      }

      if (patternMatches.isEmpty()) {
        throw new IllegalArgumentException(String.format("Did not find any jars for pattern %s", pattern));
      } else if (!allowMultiple && patternMatches.size() > 1) {
        throw new IllegalArgumentException(String.format(
            "Found multiple jars for pattern %s: %s",
            pattern,
            StringUtils.join(patternMatches, JAR_SEPARATOR)
        ));
      }
      patternMatches.clear();
    }

    appendJars(conf, allMatches);
  }

  private static void appendJars(Configuration conf, Collection<String> appendJars) {
    if (appendJars.isEmpty()) {
      return;
    }

    String addition = StringUtils.join(appendJars, JAR_SEPARATOR);

    // Get previously configured jars (if any)
    String tmpjars = conf.get(TMP_JARS_PROPERTY);
    tmpjars = StringUtils.isEmpty(tmpjars) ? addition : tmpjars + JAR_SEPARATOR + addition;

    conf.set(TMP_JARS_PROPERTY, tmpjars);
  }

  private MapreduceUtils() {
    // Instantiation is prohibited
  }
}
