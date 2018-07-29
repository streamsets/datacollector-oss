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
package com.streamsets.pipeline.stage.origin.hdfs;

import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.sdk.PushSourceRunner;
import com.streamsets.pipeline.stage.origin.spooldir.AvroSpoolDirSourceTestUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TestHdfsSource {
  HdfsSourceConfigBean hdfsConfig;

  @Before
  public void setup() {
    hdfsConfig = new HdfsSourceConfigBean();
    hdfsConfig.hdfsUri = "file:///";
    hdfsConfig.hdfsConfigs = new ArrayList<>();
  }

  private HdfsSource createSource() {
    SpoolDirConfigBean conf = AvroSpoolDirSourceTestUtil.getConf();
    return new HdfsSource(conf, hdfsConfig);
  }

  @Test
  public void testNullOffset() throws Exception {
    HdfsSource source = createSource();

    PushSourceRunner runner = new PushSourceRunner.Builder(HdfsDSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    Map<String, String> offset = new HashMap<>();
    offset.put("offset", null);
    runner.runProduce(offset, 10, output -> { });
    runner.setStop();
  }

}
