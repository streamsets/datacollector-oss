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
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.lib.dirspooler.WrappedFile;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget;
import com.streamsets.pipeline.stage.origin.hdfs.spooler.HdfsFile;
import com.streamsets.pipeline.stage.origin.spooldir.BaseTestCsvSpoolDirSource;
import com.streamsets.pipeline.stage.origin.spooldir.CsvSpoolDirSourceTestUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Before;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TestCsvSpoolDirSource extends BaseTestCsvSpoolDirSource {
  HdfsSourceConfigBean hdfsConfig;

  @Before
  public void setup() {
    clazz = HdfsDSource.class;

    hdfsConfig = new HdfsSourceConfigBean();
    hdfsConfig.hdfsUri = "file:///";
    hdfsConfig.hdfsConfigs = new ArrayList<>();


    Target.Context context = ContextInfoCreator.createTargetContext(
        HdfsDTarget.class,
        "n",
        false,
        OnRecordError.TO_ERROR,
        null);
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    hdfsConfig.init(context, issues);
  }

  protected WrappedFile createDelimitedFile() throws Exception {
    File f = CsvSpoolDirSourceTestUtil.createDelimitedFile();
    Path path = new Path(f.getAbsolutePath());
    return new HdfsFile(hdfsConfig.getFileSystem(), path);
  }

  protected WrappedFile createCustomDelimitedFile() throws Exception {
    File f = CsvSpoolDirSourceTestUtil.createCustomDelimitedFile();
    Path path = new Path(f.getAbsolutePath());
    return new HdfsFile(hdfsConfig.getFileSystem(), path);
  }

  protected WrappedFile createSomeRecordsTooLongFile() throws Exception {
    File f = CsvSpoolDirSourceTestUtil.createSomeRecordsTooLongFile();
    Path path = new Path(f.getAbsolutePath());
    return new HdfsFile(hdfsConfig.getFileSystem(), path);
  }

  protected WrappedFile createCommentFile() throws Exception {
    File f = CsvSpoolDirSourceTestUtil.createCommentFile();
    Path path = new Path(f.getAbsolutePath());
    return new HdfsFile(hdfsConfig.getFileSystem(), path);
  }

  protected WrappedFile createEmptyLineFile() throws Exception {
    File f = CsvSpoolDirSourceTestUtil.createEmptyLineFile();
    Path path = new Path(f.getAbsolutePath());
    return new HdfsFile(hdfsConfig.getFileSystem(), path);
  }

  protected HdfsSource createSource(
      CsvMode mode,
      CsvHeader header,
      char delimiter,
      char escape,
      char quote,
      boolean commentsAllowed,
      char comment,
      boolean ignoreEmptyLines,
      int maxLen,
      CsvRecordType csvRecordType,
      String filePath,
      String pattern,
      PostProcessingOptions postProcessing,
      int batchSize) {

    SpoolDirConfigBean conf = CsvSpoolDirSourceTestUtil.getConf(
        mode,
        header,
        delimiter,
        escape,
        quote,
        commentsAllowed,
        comment,
        ignoreEmptyLines,
        maxLen,
        csvRecordType,
        filePath,
        pattern,
        postProcessing,
        batchSize
    );

    this.spoolDir = conf.spoolDir;
    return new HdfsSource(conf, hdfsConfig);
  }
}
