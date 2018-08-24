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
 * WITHOUT WARRANTIES OR ROUTER OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.httprouter;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HttpRouterProcessor extends RecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(HttpRouterProcessor.class);
  private final List<HttpRouterLaneConfig> routerLaneConfigs;

  HttpRouterProcessor(List<HttpRouterLaneConfig> routerLaneConfigs) {
    this.routerLaneConfigs = routerLaneConfigs;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (CollectionUtils.isEmpty(routerLaneConfigs)) {
      issues.add(getContext().createConfigIssue(
          Groups.ROUTER.name(),
          "routerLaneConfigs",
          Errors.HTTP_ROUTER_00
      ));
    } else if (getContext().getOutputLanes().size() != routerLaneConfigs.size()) {
      issues.add(getContext().createConfigIssue(
          Groups.ROUTER.name(),
          "routerLaneConfigs",
          Errors.HTTP_ROUTER_01,
          routerLaneConfigs.size(),
          getContext().getOutputLanes().size()
      ));
    }

    return issues;
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    String httpMethod = record.getHeader().getAttribute("method");
    String path = record.getHeader().getAttribute("path");
    boolean matched = false;
    for(HttpRouterLaneConfig laneConfig: routerLaneConfigs) {
      if (laneConfig.httpMethod.getLabel().equalsIgnoreCase(httpMethod) &&
          (path != null && path.contains(laneConfig.pathParam))) {
        batchMaker.addRecord(record, laneConfig.outputLane);
        matched = true;
      }
    }
    if (!matched) {
      String errorMsg = Utils.format(
          Errors.HTTP_ROUTER_02.getMessage(),
          record.getHeader().getSourceId(),
          httpMethod,
          path
      );
      getContext().toError(record, errorMsg);
      LOG.error(errorMsg);
    }
  }

}
