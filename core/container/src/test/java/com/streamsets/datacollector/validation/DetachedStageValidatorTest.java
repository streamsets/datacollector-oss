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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.validation;

import com.streamsets.datacollector.config.DetachedStageConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DetachedStageValidatorTest {

  @Test
  public void basicTest() {
    StageLibraryTask stageLibrary = MockStages.createStageLibrary();
    StageConfiguration stageConf = MockStages.createProcessor("p", Collections.emptyList(), Collections.emptyList());

    DetachedStageValidator validator = new DetachedStageValidator(stageLibrary, new DetachedStageConfiguration(stageConf));

    StageConfiguration validatedStageConf = validator.validate().getStageConfiguration();

    assertNotNull(validatedStageConf);
    assertEquals(0, validator.issues.getIssueCount());
  }
}
