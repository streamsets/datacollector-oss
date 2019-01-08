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
package com.streamsets.pipeline.stage.devtest;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.sdk.service.SdkJsonDataFormatParserService;
import com.streamsets.pipeline.stage.devtest.rawdata.RawDataDSource;
import com.streamsets.pipeline.stage.devtest.rawdata.RawDataSource;
import org.junit.Assert;
import org.junit.Test;

public class TestRawDataSource {

  @Test
  public void testStopAfterFirstBatch() throws StageException {
    RawDataSource origin = new RawDataSource("{}", "",  true);

    SourceRunner runner = new SourceRunner.Builder(RawDataDSource.class, origin)
        .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
        .addOutputLane("a")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1);
      // New offset should be null since stopAfterFirstBatch is selected
      Assert.assertNull(output.getNewOffset());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testEventGeneration() throws StageException {
    RawDataSource origin = new RawDataSource("{}", "{}",  true);

    SourceRunner runner = new SourceRunner.Builder(RawDataDSource.class, origin)
        .addService(DataFormatParserService.class, new SdkJsonDataFormatParserService())
        .addOutputLane("a")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1);
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertEquals(1, runner.getEventRecords().size());
    } finally {
      runner.runDestroy();
    }
  }

}
