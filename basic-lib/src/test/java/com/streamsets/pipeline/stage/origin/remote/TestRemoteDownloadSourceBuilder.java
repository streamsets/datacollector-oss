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

package com.streamsets.pipeline.stage.origin.remote;

import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.stage.connection.remote.Authentication;
import com.streamsets.pipeline.stage.connection.remote.Protocol;
import com.streamsets.pipeline.lib.util.SystemClock;
import com.streamsets.pipeline.stage.connection.remote.RemoteConnection;

import java.util.Locale;

public class TestRemoteDownloadSourceBuilder {

  protected static final String TESTUSER = "testuser";
  protected static final String TESTPASS = "testpass";

  private final Scheme scheme;
  private final int port;

  private String remoteHost;
  private Protocol protocol;
  private boolean userDirIsRoot;
  private DataFormat dataFormat;
  private String errorArchive;
  private boolean processSubDirectories;
  private FilePatternMode filePatternMode;
  private String filePattern;
  private int batchSize;
  private String initialFile;
  private PostProcessingOptions postProcessing;
  private String archiveDir;
  private boolean archiveDirUserDirIsRoot;
  private FileDelayer fileDelayer;

  private Compression dataFormatCompression;
  private String filePatternInArchive;

  public enum Scheme {
    sftp, ftp, ftps
  }

  public TestRemoteDownloadSourceBuilder(Scheme scheme, int port) {
    this.scheme = scheme;
    this.port = port;
    // init defaults
    this.remoteHost = scheme.name() + "://localhost:" + port + "/";
    this.protocol = Protocol.valueOf(scheme.name().toUpperCase(Locale.ROOT));
    this.userDirIsRoot = true;
    this.dataFormat = DataFormat.JSON;
    this.errorArchive = null;
    this.processSubDirectories = false;
    this.filePatternMode = FilePatternMode.GLOB;
    this.filePattern = "*";
    this.batchSize = 1000;
    this.initialFile = "";
    this.postProcessing = PostProcessingOptions.NONE;
    this.archiveDir = null;
    this.archiveDirUserDirIsRoot = true;
    this.dataFormatCompression = Compression.NONE;
    this.filePatternInArchive = "";
    this.fileDelayer = new FileDelayer(new SystemClock(), 0);
  }

  public TestRemoteDownloadSourceBuilder withRemoteHost(String remoteHost) {
    this.remoteHost = remoteHost;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withProtocol(Protocol protocol) {
    this.protocol = protocol;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withUserDirIsRoot(boolean userDirIsRoot) {
    this.userDirIsRoot = userDirIsRoot;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withDataFormat(DataFormat dataFormat) {
    this.dataFormat = dataFormat;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withErrorArchive(String errorArchive) {
    this.errorArchive = errorArchive;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withProcessSubDirectories(boolean processSubDirectories) {
    this.processSubDirectories = processSubDirectories;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withFilePatternMode(FilePatternMode filePatternMode) {
    this.filePatternMode = filePatternMode;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withFilePattern(String filePattern) {
    this.filePattern = filePattern;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withInitialFile(String initialFile) {
    this.initialFile = initialFile;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withPostProcessing(PostProcessingOptions postProcessingOptions) {
    this.postProcessing = postProcessingOptions;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withArchiveDir(String archiveDir) {
    this.archiveDir = archiveDir;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withArchiveDirUserDirIsRoot(boolean archiveDirUserDirIsRoot) {
    this.archiveDirUserDirIsRoot = archiveDirUserDirIsRoot;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withFileDelayer(FileDelayer fileDelayer) {
    this.fileDelayer = fileDelayer;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withDataFormatCompression(Compression dataFormatCompression) {
    this.dataFormatCompression = dataFormatCompression;
    return this;
  }

  public TestRemoteDownloadSourceBuilder withFilePatternInArchive(String filePatternInArchive) {
    this.filePatternInArchive = filePatternInArchive;
    return this;
  }

  public RemoteDownloadSource build() {
    RemoteDownloadConfigBean configBean = new RemoteDownloadConfigBean();
    configBean.remoteConfig.connection = new RemoteConnection();
    configBean.remoteConfig.connection.remoteAddress = remoteHost;
    configBean.remoteConfig.connection.protocol = protocol;
    configBean.remoteConfig.userDirIsRoot = userDirIsRoot;
    configBean.remoteConfig.connection.credentials.username = () -> TESTUSER;
    configBean.remoteConfig.connection.credentials.auth = Authentication.PASSWORD;
    configBean.remoteConfig.connection.credentials.password = () -> TESTPASS;
    configBean.remoteConfig.connection.credentials.strictHostChecking = false;
    configBean.dataFormat = dataFormat;
    configBean.errorArchiveDir = errorArchive;
    configBean.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    configBean.processSubDirectories = processSubDirectories;
    configBean.filePatternMode = filePatternMode;
    configBean.filePattern = filePattern;
    configBean.basic.maxBatchSize = batchSize;
    configBean.initialFileToProcess = initialFile;
    configBean.postProcessing = postProcessing;
    configBean.archiveDir = archiveDir;
    configBean.archiveDirUserDirIsRoot = archiveDirUserDirIsRoot;
    configBean.dataFormatConfig.compression = this.dataFormatCompression;
    configBean.dataFormatConfig.filePatternInArchive = this.filePatternInArchive;

    return new RemoteDownloadSource(configBean, this.fileDelayer);
  }
}
