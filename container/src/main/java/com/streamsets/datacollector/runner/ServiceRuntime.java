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
import com.streamsets.pipeline.api.service.Service;

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
}
