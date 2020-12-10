/*
 * Copyright 2020 StreamSets Inc.
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

package com.streamsets.datacollector.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Engine {
  private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

  public static final String ENGINE_3_x = "3.x";
  public static final String ENGINE_NEXT = "next";

  private Engine() {
    //Empty constructor
  }

  public static final String ENGINE_FILE_NAME = "engine.properties";
  public static final String ENGINE_PROPERTY = "engine.type";

  public static String getEngineType() {
    Properties properties = new Properties();
    try (InputStream in = Engine.class.getClassLoader().getResourceAsStream(ENGINE_FILE_NAME)) {
      properties.load(in);
    } catch (IOException e) {
      LOG.error("Cannot retrieve engine type, defaulting to Next");
    }

    // For the time being we will default it to next since it will be more restrictive
    return properties.containsKey(ENGINE_PROPERTY) ? properties.getProperty(ENGINE_PROPERTY) : ENGINE_NEXT;
  }
}
