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
package com.streamsets.pipeline.stage;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.lib.hive.HiveConfigBean;

import java.util.Collections;
import java.util.UUID;

public final class HiveTestUtil {
  public static final String WAREHOUSE_DIR = "/user/hive/warehouse";

  private static final String HOSTNAME = "localhost";
  private static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
  private static String CONF_DIR = "target/" + UUID.randomUUID().toString();

  private HiveTestUtil() {
  }

  /**
   * Generates HiveConfigBean with pre-filled values.
   */
  public static HiveConfigBean getHiveConfigBean() {
    HiveConfigBean hiveConfigBean = new HiveConfigBean();
    hiveConfigBean.confDir = CONF_DIR;
    hiveConfigBean.hiveJDBCDriver = HIVE_JDBC_DRIVER;
    hiveConfigBean.additionalConfigProperties = Collections.emptyMap();
    hiveConfigBean.hiveJDBCUrl = getHiveJdbcUrl();
    return hiveConfigBean;
  }

  /**
   * Returns Dummy HS2 JDBC URL for tests.
   */
  public static String getHiveJdbcUrl() {
    return Utils.format(
        "jdbc:hive2://{}:{}/default;user={}",
        HOSTNAME, 10000,
        System.getProperty("user.name")
    );
  }
}
