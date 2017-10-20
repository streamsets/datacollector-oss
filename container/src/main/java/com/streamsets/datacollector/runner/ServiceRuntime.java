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
package com.streamsets.datacollector.runner;

import com.streamsets.datacollector.creation.PipelineBean;
import com.streamsets.datacollector.creation.ServiceBean;
import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.service.Service;

import java.util.List;

public class ServiceRuntime {
  private final PipelineBean pipelineBean;
  private final ServiceBean serviceBean;
  private Service.Context context;

  public ServiceRuntime(
      PipelineBean pipelineBean,
      ServiceBean serviceBean
  ) {
    this.pipelineBean = pipelineBean;
    this.serviceBean = serviceBean;
  }

  public void setContext(Service.Context context) {
    this.context = context;
  }

  public List<Issue> init() {
    return LambdaUtil.withClassLoader(
        serviceBean.getDefinition().getStageClassLoader(),
        () -> (List)serviceBean.getService().init(context)
    );
  }

  public void destroy() {
    LambdaUtil.withClassLoader(
      serviceBean.getDefinition().getStageClassLoader(),
      () -> { serviceBean.getService().destroy(); return null; }
    );
  }
}
