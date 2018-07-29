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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.CsvRecordType;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.LocalFileSystem;
import com.streamsets.pipeline.lib.dirspooler.PathMatcherMode;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.lib.dirspooler.WrappedFile;
import org.junit.Before;

import java.io.File;

public class TestCsvSpoolDirSource extends BaseTestCsvSpoolDirSource {
  @Before
  public void setup() {
    clazz = SpoolDirDSource.class;
  }

  protected WrappedFile createDelimitedFile() throws Exception {
    File f = CsvSpoolDirSourceTestUtil.createDelimitedFile();
    return new LocalFileSystem("*", PathMatcherMode.GLOB).getFile(f.getAbsolutePath());
  }

  protected WrappedFile createCustomDelimitedFile() throws Exception {
    File f = CsvSpoolDirSourceTestUtil.createCustomDelimitedFile();
    return new LocalFileSystem("*", PathMatcherMode.GLOB).getFile(f.getAbsolutePath());
  }

  protected WrappedFile createSomeRecordsTooLongFile() throws Exception {
    File f = CsvSpoolDirSourceTestUtil.createSomeRecordsTooLongFile();
    return new LocalFileSystem("*", PathMatcherMode.GLOB).getFile(f.getAbsolutePath());
  }

  protected WrappedFile createCommentFile() throws Exception {
    File f = CsvSpoolDirSourceTestUtil.createCommentFile();
    return new LocalFileSystem("*", PathMatcherMode.GLOB).getFile(f.getAbsolutePath());
  }

  protected WrappedFile createEmptyLineFile() throws Exception {
    File f = CsvSpoolDirSourceTestUtil.createEmptyLineFile();
    return new LocalFileSystem("*", PathMatcherMode.GLOB).getFile(f.getAbsolutePath());
  }

  protected SpoolDirSource createSource(
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
    PostProcessingOptions postProcessing) {

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
        postProcessing
    );

    this.spoolDir = conf.spoolDir;
    return new SpoolDirSource(conf);
  }
}
