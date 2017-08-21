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

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

public abstract class DExecutor extends DStage<Executor.Context> implements Executor {

  protected abstract Executor createExecutor();

  @Override
  Stage<Executor.Context> createStage() {
    return createExecutor();
  }

  @Override
  public final void write(Batch batch) throws StageException {
    ((Executor)getStage()).write(batch);
  }

}
