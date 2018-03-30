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

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.lib.dirspooler.WrappedFile;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget;
import com.streamsets.pipeline.stage.origin.hdfs.spooler.HdfsFile;
import com.streamsets.pipeline.stage.origin.spooldir.AvroSpoolDirSourceTestUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TestAvroSpoolDirSource {
  HdfsSourceConfigBean hdfsConfig;

  @Before
  public void setup() {
    hdfsConfig = new HdfsSourceConfigBean();
    hdfsConfig.hdfsUri = "file:///";
    hdfsConfig.hdfsConfigs = new ArrayList<>();
  }

  private WrappedFile createAvroDataFile() throws Exception {
    File f = AvroSpoolDirSourceTestUtil.createAvroDataFile();
    Path path = new Path(f.getAbsolutePath());

    Target.Context context = ContextInfoCreator.createTargetContext(
        HdfsDTarget.class,
        "n",
        false,
        OnRecordError.TO_ERROR,
        null);
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    hdfsConfig.init(context, issues);

    return new HdfsFile(hdfsConfig.getFileSystem(), path);
  }

  private HdfsSource createSource() {
    SpoolDirConfigBean conf = AvroSpoolDirSourceTestUtil.getConf();
    return new HdfsSource(conf, hdfsConfig);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    HdfsSource source = createSource();
    AvroSpoolDirSourceTestUtil.testProduceFullFile(HdfsDSource.class, source, createAvroDataFile());
  }

  @Test
  public void testProduceLessThanFile() throws Exception {
    HdfsSource source = createSource();
    AvroSpoolDirSourceTestUtil.testProduceLessThanFile(HdfsDSource.class, source, createAvroDataFile());
  }
}
