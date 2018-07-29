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
package com.streamsets.datacollector.security;

/**
 * Various shared configuration constants for all Hadoop components.
 */
public class HadoopConfigConstants {

  /**
   * Used in sdc.properties as stage config property. If set to true, then Hadoop components will always impersonate
   * current user rather then using the field "Hadoop user".
   */
  public static final String IMPERSONATION_ALWAYS_CURRENT_USER = "hadoop.always.impersonate.current.user";

  /**
   * Used in sdc.properties as stage config property. If set to true, then user name will be lower cased when used
   * when impersonated.
   */
  public static final String LOWERCASE_USER = "hadoop.always.lowercase.user";

}
