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
package com.streamsets.pipeline.configurablestage;

import com.streamsets.pipeline.api.Stage;

import java.util.List;

public abstract class DStage<C extends Stage.Context> implements Stage<C> {
  private Stage<C> stage;

  public Stage<C> getStage() {
    return stage;
  }

  abstract Stage<C> createStage();

  @Override
  public final List<ConfigIssue> init(Info info, C context) {
    if(stage == null) {
      stage = createStage();
    }
    return stage.init(info, context);
  }

  @Override
  public final void destroy() {
    if(stage != null) {
      stage.destroy();
    }
  }

}
