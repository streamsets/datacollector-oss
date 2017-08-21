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

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

public abstract class DSource extends DStage<Source.Context> implements Source {

  protected abstract Source createSource();

  @Override
  Stage<Source.Context> createStage() {
    return createSource();
  }

  public Source getSource() {
    return (Source)getStage();
  }

  @Override
  public final String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    return getSource().produce(lastSourceOffset, maxBatchSize, batchMaker);
  }

}
