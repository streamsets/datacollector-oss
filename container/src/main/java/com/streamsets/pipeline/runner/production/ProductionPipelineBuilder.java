/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.runner.production;

import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.runner.Pipeline;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.preview.PreviewStageLibrary;
import com.streamsets.pipeline.stagelibrary.StageLibrary;
import com.streamsets.pipeline.validation.PipelineConfigurationValidator;

public class ProductionPipelineBuilder {

  private final StageLibrary stageLib;
  private final String name;
  private final PipelineConfiguration pipelineConf;

  public ProductionPipelineBuilder(StageLibrary stageLib, String name, PipelineConfiguration pipelineConf) {
    this.stageLib = new PreviewStageLibrary(stageLib);
    this.name = name;
    this.pipelineConf = pipelineConf;
  }

  public ProductionPipeline build(ProductionPipelineRunner runner) throws PipelineRuntimeException {
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLib, name, pipelineConf);
    if (!validator.validate()) {
      throw new PipelineRuntimeException(PipelineRuntimeException.ERROR.CANNOT_PREVIEW, validator.getIssues());
    }
    Pipeline pipeline = new Pipeline.Builder(stageLib, name + ":production", pipelineConf).build(runner);
    return new ProductionPipeline(pipeline);
  }

}
