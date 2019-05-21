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
   * sdc.properties config to enable downloading updates to the knowledge base.
   */
  public final static String CONF_UPDATE_ENABLE = "antennadoctor.update.enable";
  public final static boolean DEFAULT_UPDATE_ENABLE = true;

  /**
   * sdc.properties config for initial delay (in minutes) of when to ask for updates after SDC start.
   */
  public final static String CONF_UPDATE_DELAY = "antennadoctor.update.delay";
  public final static int DEFAULT_UPDATE_DELAY = 5;

  /**
   * sdc.properties config for period between asking for updates (in minutes).
   */
  public final static String CONF_UPDATE_PERIOD = "antennadoctor.update.period";
  public final static int DEFAULT_UPDATE_PERIOD = 12*60;

  /**
   * sdc.properties config to enable downloading updates to the knowledge base.
   */
  public final static String CONF_UPDATE_URL = "antennadoctor.update.url";
  public final static String DEFAULT_UPDATE_URL = "https://antenna.streamsets.com";

  /**
   * Name of the resource file that have built-in database
   */
  public final static String FILE_DATABASE = "antenna-doctor-rules.json";

  /**
   * Directory inside data dir that holds files from Antenna Doctor.
   */
  public final static String DIR_REPOSITORY = "antennadoctor";

  /**
   * Name of file that we will scan for in case user enabled overrides.
   */
  public final static String FILE_OVERRIDE = "antenna-doctor-override.json";

  /**
   * Manifest file for the repository.
   */
  public final static String URL_MANIFEST = "manifest.json";

  /**
   * URL end point for downloading given versioned files.
   */
  public final static String URL_VERSION_END = ".json.gz";
}
