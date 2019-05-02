/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.antennadoctor;

public final class AntennaDoctorConstants {

  /**
   * sdc.properties config to enable/disable Antenna Doctor functionality.
   */
  public final static String CONF_ENABLE = "antennadoctor.enable";
  public final static boolean DEFAULT_ENABLE = true;

  /**
   * sdc.properties config to enable scanning for special override file that will be auto-loaded every 5 seconds.
   */
  public final static String CONF_OVERRIDE_ENABLE = "antennadoctor.override.enable";
  public final static boolean DEFAULT_OVERRIDE_ENABLE = false;

  /**
   * Name of the resource file that have built-in database
   */
  public final static String RESOURCE_DATABASE = "antenna-doctor-rules.json";

  /**
   * Directory inside data dir that holds files from Antenna Doctor.
   */
  public final static String DIR_REPOSITORY = "antennadoctor";

  /**
   * Name of file that we will scan for in case user enabled overrides.
   */
  public final static String FILE_OVERRIDE = "antenna-doctor-override.json";
}
