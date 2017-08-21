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
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;

import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsFileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.Assert;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.TimeZone;

public final class RecordWriterManagerTestBuilder {
  private URI hdfsUri = new URI("file:///");
  private Configuration hdfsConf = new HdfsConfiguration();
  private String uniquePrefix = "prefix";
  private String fileNameSuffix = "";
  private boolean dirPathTemplateInHeader = false;
  private String dirPathTemplate;
  private TimeZone timeZone = TimeZone.getTimeZone("UTC");
  private long cutOffSecs = 2;
  private long cutOffSize = 10000;
  private long cutOffRecords = 2;
  private HdfsFileType fileType = HdfsFileType.SEQUENCE_FILE;
  private CompressionCodec compressionCodec = new DefaultCodec();
  private SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.BLOCK;
  private String keyEL = "${uuid()}";
  private boolean rollIfHeader = false;
  private  String rollHeaderName = "roll";
  private String config = "dirPathTemplate";
  private String fileNameEL = "";
  private WholeFileExistsAction wholeFileExistsAction = WholeFileExistsAction.TO_ERROR;
  private String permissionEL = "";
  private DataGeneratorFactory generatorFactory = new TestActiveRecordWriters.DummyDataGeneratorFactory(null);
  private Target.Context context;

  public RecordWriterManagerTestBuilder() throws URISyntaxException {
    ((DefaultCodec)compressionCodec).setConf(hdfsConf);
  }

  public RecordWriterManagerTestBuilder context(Target.Context context) {
    this.context = context;
    return this;
  }

  public RecordWriterManagerTestBuilder generatorFactory(DataGeneratorFactory factory) {
    this.generatorFactory = factory;
    return this;
  }

  public RecordWriterManagerTestBuilder hdfsUri(URI hdfsUri) {
    this.hdfsUri = hdfsUri;
    return this;
  }

  public RecordWriterManagerTestBuilder hdfsConf(Configuration hdfsConf) {
    this.hdfsConf = hdfsConf;
    return this;
  }

  public RecordWriterManagerTestBuilder uniquePrefix(String uniquePrefix) {
    this.uniquePrefix = uniquePrefix;
    return this;
  }

  public RecordWriterManagerTestBuilder fileNameSuffix(String fileNameSuffix) {
    this.fileNameSuffix = fileNameSuffix;
    return this;
  }

  public RecordWriterManagerTestBuilder dirPathTemplateInHeader(boolean dirPathTemplateInHeader) {
    this.dirPathTemplateInHeader = dirPathTemplateInHeader;
    return this;
  }

  public RecordWriterManagerTestBuilder dirPathTemplate(String dirPathTemplate) {
    this.dirPathTemplate = dirPathTemplate;
    return this;
  }

  public RecordWriterManagerTestBuilder timeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
    return this;
  }

  public RecordWriterManagerTestBuilder cutOffSecs(long cutOffSecs) {
    this.cutOffSecs = cutOffSecs;
    return this;
  }

  public RecordWriterManagerTestBuilder cutOffSizeBytes(long cutOffSize) {
    this.cutOffSize = cutOffSize;
    return this;
  }

  public RecordWriterManagerTestBuilder cutOffRecords(long cutOffRecords) {
    this.cutOffRecords = cutOffRecords;
    return this;
  }

  public RecordWriterManagerTestBuilder fileType(HdfsFileType fileType) {
    this.fileType = fileType;
    return this;
  }

  public RecordWriterManagerTestBuilder compressionCodec(CompressionCodec compressionCodec) {
    this.compressionCodec = compressionCodec;
    return this;
  }

  public RecordWriterManagerTestBuilder compressionType(SequenceFile.CompressionType compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  public RecordWriterManagerTestBuilder keyEl(String keyEL) {
    this.keyEL = keyEL;
    return this;
  }

  public RecordWriterManagerTestBuilder rollIfHeader(boolean rollIfHeader) {
    this.rollIfHeader = rollIfHeader;
    return this;
  }

  public RecordWriterManagerTestBuilder rollHeaderName(String rollHeaderName) {
    this.rollHeaderName = rollHeaderName;
    return this;
  }

  public RecordWriterManagerTestBuilder config(String config) {
    this.config = config;
    return this;
  }

  public RecordWriterManagerTestBuilder fileNameEL(String fileNameEL) {
    this.fileNameEL = fileNameEL;
    return this;
  }

  public RecordWriterManagerTestBuilder wholeFileExistsAction(WholeFileExistsAction wholeFileExistsAction) {
    this.wholeFileExistsAction = wholeFileExistsAction;
    return this;
  }

  public RecordWriterManagerTestBuilder permissionEL(String permissionEL) {
    this.permissionEL = permissionEL;
    return this;
  }

  public RecordWriterManager build() throws IOException {
    RecordWriterManager mgr = new RecordWriterManager(
      FileSystem.get(hdfsUri, hdfsConf),
      hdfsConf,
      uniquePrefix,
      fileNameSuffix,
      dirPathTemplateInHeader,
      dirPathTemplate,
      timeZone,
      cutOffSecs,
      cutOffSize,
      cutOffRecords,
      fileType,
      compressionCodec,
      compressionType,
      keyEL,
      rollIfHeader,
      rollHeaderName,
      fileNameEL,
      wholeFileExistsAction,
      permissionEL,
      generatorFactory,
      context,
      config
    );

    Assert.assertTrue(mgr.validateDirTemplate("g", "dirPathTemplate", "dirPathTemplate", new ArrayList<Stage.ConfigIssue>()));

    return mgr;
  }
}
