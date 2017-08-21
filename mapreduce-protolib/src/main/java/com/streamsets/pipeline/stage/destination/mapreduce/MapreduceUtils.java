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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Set;

public final class MapreduceUtils {

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
      additinonalJars.add(jarForClass(klass));
    }
    String addition = StringUtils.join(additinonalJars, ",");

    // Get previously configured jars (if any)
    String tmpjars = conf.get("tmpjars");
    tmpjars = StringUtils.isEmpty(tmpjars) ? addition : tmpjars + "," + addition;

    conf.set("tmpjars", tmpjars);
  }

  private MapreduceUtils() {
    // Instantiation is prohibited
  }
}
