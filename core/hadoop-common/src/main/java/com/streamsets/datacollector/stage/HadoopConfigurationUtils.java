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
package com.streamsets.datacollector.stage;

import org.apache.hadoop.conf.Configuration;

public class HadoopConfigurationUtils {
  private static final String HADOOP_SUBJECT_TREAT_EXTERNAL = "hadoop.treat.subject.external";

  public static void configureHadoopTreatSubjectExternal(Configuration conf) {
    // Not using constant to make this code compile even for stage libraries that do
    // not have HADOOP-13805 available.
    conf.setBoolean(HADOOP_SUBJECT_TREAT_EXTERNAL, true);
  }
}
