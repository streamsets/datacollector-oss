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

import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.Utils;

public abstract class DTargetOffsetCommitTrigger extends DTarget implements OffsetCommitTrigger {
  protected OffsetCommitTrigger offsetCommitter;
  protected Target target;

  @Override
  Stage<Target.Context> createStage() {
    target = (Target) super.createStage();
    if (!(target instanceof OffsetCommitTrigger)) {
      throw new RuntimeException(
        Utils.format(
            "Stage '{}' does not implement '{}'",
            target.getClass().getName(),
            OffsetCommitTrigger.class.getName()
        )
      );
    }
    offsetCommitter = (OffsetCommitTrigger) target;
    return target;
  }

  @Override
  public final boolean commit() {
    return offsetCommitter.commit();
  }
}
