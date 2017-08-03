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

package com.streamsets.pipeline.stage.plugin.navigator;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.EndPointType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NavigatorParameters {
  private static final Logger LOG = LoggerFactory.getLogger(NavigatorParameters.class);
  private SourceType sourceType;
  private EntityType entityType;

  NavigatorParameters(EndPointType type) {
    switch (type) {
      case HIVE:
        sourceType = SourceType.HIVE;
        entityType = EntityType.DATABASE;
        break;

      case HBASE:
      case JDBC:
      case KUDU:
        sourceType = SourceType.SDK;
        entityType = EntityType.TABLE;
        break;

      case DEVDATA:
      case HDFS:
        // TODO: fix this when the Navigator API is fixed.  should be HDFS, FILE
        sourceType = SourceType.SDK;
        entityType = EntityType.DATASET;
        break;

      case OTHER:
        sourceType = SourceType.SDK;
        entityType = EntityType.OPERATION;
        break;

      default:
        LOG.error(Errors.NAVIGATOR_03.getMessage(), type);
        throw new IllegalArgumentException(Utils.format(Errors.NAVIGATOR_03.getMessage(), type));
    }

  }

  public SourceType getSourceType() {
    return sourceType;
  }

  public EntityType getEntityType() {
    return entityType;
  }
}
