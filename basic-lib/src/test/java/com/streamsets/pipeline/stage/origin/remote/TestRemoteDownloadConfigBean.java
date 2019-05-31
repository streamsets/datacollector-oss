/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.config.DataFormat;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestRemoteDownloadConfigBean {

  @Test
  public void testPostProcessingTriggeredByValueHasAllButWholeFile() throws Exception {
    ConfigDef configDef = RemoteDownloadConfigBean.class.getDeclaredField("postProcessing")
        .getAnnotation(ConfigDef.class);
    String[] expectedFormats = Arrays.stream(DataFormat.values())
        .filter(dataFormat -> dataFormat != DataFormat.WHOLE_FILE)
        .map(dataFormat -> dataFormat.name())
        .toArray(String[]::new);
    Assert.assertArrayEquals(
        "RemoteDownloadConfigBean#postProcessing should have all DataFormats except for WHOLE_FILE ("
            + Arrays.toString(expectedFormats) + ") but found " + Arrays.toString(configDef.triggeredByValue()),
        expectedFormats,
        configDef.triggeredByValue());
  }

}