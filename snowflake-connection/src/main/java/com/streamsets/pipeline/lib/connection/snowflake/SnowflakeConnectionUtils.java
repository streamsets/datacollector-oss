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

package com.streamsets.pipeline.lib.connection.snowflake;

import com.streamsets.pipeline.api.StageException;

import java.util.Properties;

public class SnowflakeConnectionUtils {

  public String getJdbcUrl(
      String customUrl,
      SnowflakeRegion snowflakeRegion,
      String customSnowflakeRegion,
      String account
  ) throws StageException {
    String regionId = getRegionId(snowflakeRegion, customSnowflakeRegion);
    if (SnowflakeRegion.CUSTOM_URL.getId().equals(regionId)) {
      return customUrl;
    } else {
      return String.format("jdbc:snowflake://%s", getRegionHost(regionId, account));
    }
  }

  private String getRegionId(SnowflakeRegion snowflakeRegion, String customSnowflakeRegion) {
    return (snowflakeRegion != SnowflakeRegion.OTHER) ?
        snowflakeRegion.getId() : customSnowflakeRegion.toLowerCase();
  }

  private String getRegionHost(String regionId, String account) throws StageException {
    if (SnowflakeRegion.US_WEST_2.getId().equals(regionId)) {
      return String.format("%s.snowflakecomputing.com", account);
    } else {
      return String.format("%s.%s.snowflakecomputing.com", account, regionId);
    }
  }

  public Properties getVerifierJdbcProperties(String user, String password) throws Exception {
    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    return props;
  }
}
