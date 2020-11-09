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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parse dependency name and version.
 */
public class DependencyParser {

  private static final Logger LOG = LoggerFactory.getLogger(DependencyParser.class);

  /**
   * Classifiers that are not interesting.
   */
  private static String CLASSIFIERS = "(-hadoop2|-shaded-protobuf|-tests|-native|\\.Final(-linux-x86_64)?|-shaded|-bin|\\.jre[0-9]|-api|-empty-to-avoid-conflict-with-guava|-ccs)?";

  /**
   * Various version suffixes that we support.
   */
  private static String VERSION_SUFFIXES = "-b[0-9]+|-M[0-9]+|-m[0-9]|_[0-9]|-pre[0-9]+|\\.RELEASE|-incubating|-beta|-indy|-SNAPSHOT|\\.GA|-GA|\\.hwx|[-\\.]cloudera\\.?[0-9]|-jhyde|a|-cubrid|\\.Fork[0-9]+|m|-jre|-spark[0-9]\\.[0-9]+|-patched|-android|-nohive|.pr1";

  /**
   * Various supported version specifications
   */
  private static String[] VERSION_PATTERNS = new String[] {
    // Maven resolved snapshots
    "-([0-9.]+-[0-9]{8}\\.[0-9]{6}-[0-9]+)",
    // Hortonworks
    "-([0-9]+(\\.[0-9]+){4,}-[0-9]+)",
    // CDH
    "-([0-9.]+-cdh[0-9.]+(-beta1)?)",
    "-([0-9.]+-kafka-[0-9.]+)",
    // MapR
    "-([0-9.]+-mapr-beta)",
    "-([0-9.]+-mapr-[0-9.]+(-beta|-standalone)?)",
    "-([0-9.]+-mapr)",
    // Time based (like '3.0.0.v201112011016')
    "-([0-9.]+\\.v[0-9.]+)",
    // With commit id at the end (like '1.2.0-3f79e055')
    "-([0-9.]+-[a-f0-9]{6,8})",
    // PostgreSQL JDBC driver numbering scheme
    "-([0-9.]+-[0-9]+)\\.jdbc[0-9]",

    // Most basic version specification
    "-([0-9.]+(" + VERSION_SUFFIXES + ")?)"
  };

  /**
   * Various jar name prefixes
   */
  private static String[] PATTERN_PREFIXES = new String[] {
    // Libraries that ships multiple jars with different name, but all versions must match
    "(antlr)-.*",
    "(antlr4)-.*",
    "(asm).*",
    "(atlas).*",
    "(avatica).*",
    "(avro).*",
    "(aws-java-sdk).*",
    "(calcite).*",
    "(geronimo).*",
    "(groovy).*",
    "(grpc(?!-google-cloud-pubsub-v1|-google-common-protos)).*",
    "(hadoop).*",
    "(hamcrest).*",
    "(hbase).*",
    "(hive(?!-bridge)).*",
    "(jackson).*",
    "(jersey).*",
    "(jetty).*",
    "(json4s).*",
    "(kafka(?!-avro-serializer|-schema-registry-client)).*",
    "(log4j).*",
    "(mapr).*",
    "(metrics).*",
    "(netty(?!-tcnative-boringssl-static)).*",
    "(orc).*",
    "(parquet).*",
    "(spark).*",
    "(streamsets-datacollector-dataprotector).*",
    "(streamsets(?!-datacollector-spark-api)).*",
    "(swagger).*",
    "(tachyon).*",
    "(twill).*",
    "(websocket).*",

    // Oracle cause Oracle is always special

    // Most basic name (last resort)
    "([A-Za-z_.0-9-]+)"
  };

  /**
   * Whole patterns for some special libraries.
   */
  private static String[] SPECIAL_PATTERNS = new String[] {
    // Oracle needs to be always special
    "(ojdbc)([0-9]+)\\.jar"
  };

  /**
   * Special cases that we have to deal with outside of general regular expressions.
   */
  private static Map<String, Dependency> SPECIAL_CASES = new HashMap<>();
  static {
    // Jython
    SPECIAL_CASES.put("jython.jar", new Dependency("jython", ""));
    // MapR
    SPECIAL_CASES.put("mail.jar", new Dependency("mail", ""));
    // Various JDBC drivers from vendors who don't follow usual maven scheme
    SPECIAL_CASES.put("db2jcc4.jar", new Dependency("db2jcc4", ""));
    SPECIAL_CASES.put("nzjdbc3.jar", new Dependency("nzjdbc3", ""));
    SPECIAL_CASES.put("sqljdbc4.jar", new Dependency("sqljdbc4", ""));
    SPECIAL_CASES.put("tdgssconfig.jar", new Dependency("tdgssconfig", ""));
    SPECIAL_CASES.put("terajdbc4.jar", new Dependency("terajdbc4", ""));
    SPECIAL_CASES.put("xdb6.jar", new Dependency("xdb6", ""));
  }

  /**
   * List of all supported regular expressions.
   *
   * The list will be searched start to end and first match will be considered successful and final.
   */
  private static List<Pattern> PATTERNS = new LinkedList<>();
  static {
    for(String pattern: SPECIAL_PATTERNS) {

      PATTERNS.add(Pattern.compile("^" + pattern + "$"));
    }
    for(String prefix: PATTERN_PREFIXES) {
      for(String version: VERSION_PATTERNS) {
        PATTERNS.add(Pattern.compile("^" + prefix + version + CLASSIFIERS + "\\.jar$"));
      }
    }
  }

  /**
   * Generate dependency from a jar file name.
   */
  public static Optional<Dependency> parseJarName(String sourceName, String jarName) {
    if(SPECIAL_CASES.containsKey(jarName)) {
      Dependency specialCase = SPECIAL_CASES.get(jarName);
      return Optional.of(new Dependency(sourceName, specialCase.getName(), specialCase.getVersion()));
    }

    // Go over all known patterns
    for(Pattern p : PATTERNS) {
      Matcher m = p.matcher(jarName);
      if (m.matches()) {
        LOG.trace("Applied pattern '{}' to {}", p.pattern(), jarName);
        return Optional.of(new Dependency(sourceName, m.group(1), m.group(2)));
      }
    }

    // Otherwise this jar name is unknown to us
    return Optional.empty();
  }

  /**
   * Generate dependency from a URL.
   */
  public static Optional<Dependency> parseURL(URL url) {
    return parseJarName(url.toString(), Paths.get(url.getPath()).getFileName().toString());
  }
}
