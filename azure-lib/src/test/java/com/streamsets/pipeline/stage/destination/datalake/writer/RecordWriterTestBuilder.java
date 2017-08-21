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
package com.streamsets.pipeline.stage.destination.datalake.writer;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.WholeFileExistsAction;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.destination.datalake.DataLakeDTarget;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

public class RecordWriterTestBuilder {
  ADLStoreClient client;
  DataFormat dataFormat;
  DataGeneratorFormatConfig dataFormatConfig;
  String uniquePrefix;
  String fileNameSuffix;
  String fileNameEL;
  boolean dirPathTemplateInHeader;
  boolean rollIfHeader;
  String rollHeaderName;
  long maxRecordsPerFile;
  long maxFileSize;
  WholeFileExistsAction wholeFileExistsAction;
  String authTokenEndpoint;
  String clientId;
  String clientKey;
  long idleTimeout;

  public RecordWriterTestBuilder() {
    this.client = null;
    this.dataFormat = DataFormat.TEXT;
    this.dataFormatConfig = new DataGeneratorFormatConfig();
    this.uniquePrefix = "sdc";
    this.fileNameSuffix = "txt";
    this.fileNameEL = "";
    this.dirPathTemplateInHeader = false;
    this.rollIfHeader = false;
    this.rollHeaderName = "";
    this.maxRecordsPerFile = 1000;
    this.maxFileSize = 0;
    this.wholeFileExistsAction = WholeFileExistsAction.OVERWRITE;
    this.authTokenEndpoint = "";
    this.clientId = "";
    this.clientKey = "";
    this.idleTimeout = -1L;
  }

  public RecordWriterTestBuilder uniquePrefix(String uniquePrefix) {
    this.uniquePrefix = uniquePrefix;
    return this;
  }

  public RecordWriterTestBuilder fileNameSuffix(String fileSuffix) {
    this.fileNameSuffix = fileSuffix;
    return this;
  }

  public RecordWriterTestBuilder dirPathTemplateInHeader(boolean dirPathTemplateInHeader) {
    this.dirPathTemplateInHeader = dirPathTemplateInHeader;
    return this;
  }

  public RecordWriterTestBuilder rollHeaderName(String rollHeaderName) {
    this.rollHeaderName = rollHeaderName;
    return this;
  }

  public RecordWriterTestBuilder rollIfHeader(boolean rollIfHeader) {
    this.rollIfHeader = rollIfHeader;
    return this;
  }

  public RecordWriter build() {
    return new RecordWriter(
        client,
        dataFormat,
        dataFormatConfig,
        uniquePrefix,
        fileNameSuffix,
        fileNameEL,
        dirPathTemplateInHeader,
        ContextInfoCreator.createTargetContext(
            DataLakeDTarget.class,
            "testWritersLifecycle",
            false,
            OnRecordError.TO_ERROR,
            null
        ),
        rollIfHeader,
        rollHeaderName,
        maxRecordsPerFile,
        maxFileSize,
        wholeFileExistsAction,
        authTokenEndpoint,
        clientId,
        clientKey,
        idleTimeout
    );
  }

}
