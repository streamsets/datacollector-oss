/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.startJob;

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.startJob.StartJobErrors;
import com.streamsets.pipeline.sdk.SourceRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestStartJobSource {

  @Test
  public void testEmptyJobId() throws StageException {
    Source startJobSource = new TestStartJobSourceBuilder()
        .taskName("task1")
        .baseUrl("http://invalidHost:18631")
        .jobIdConfig("", "{}")
        .build();

    SourceRunner runner = new SourceRunner.Builder(StartJobDSource.class, startJobSource)
        .addOutputLane("a")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    // Configuration value is required for job ID
    Assert.assertTrue(issues.get(0).toString().contains(StartJobErrors.START_JOB_06.name()));
  }

}
