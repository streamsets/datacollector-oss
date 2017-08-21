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

import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

public abstract class DSourceOffsetCommitter extends DSource implements OffsetCommitter {
  protected OffsetCommitter offsetCommitter;
  protected Source source;

  @Override
  Stage<Source.Context> createStage() {
    source = (Source) super.createStage();
    if (!(source instanceof OffsetCommitter)) {
      throw new RuntimeException(Utils.format("Stage '{}' does not implement '{}'", source.getClass().getName(),
                                              OffsetCommitter.class.getName()));
    }
    offsetCommitter = (OffsetCommitter) source;
    return source;
  }

  @Override
  public final void commit(String offset) throws StageException {
    offsetCommitter.commit(offset);
  }
}
