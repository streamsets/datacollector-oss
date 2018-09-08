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

public class DataLakeGeneratorManagerTestBuilder {
  private ADLStoreClient client;
  private DataFormat dataFormat;
  private DataGeneratorFormatConfig dataFormatConfig;
  private String uniquePrefix;
  private String fileNameSuffix;
  private String fileNameEL;
  private boolean dirPathTemplateInHeader;
  private boolean rollIfHeader;
  private String rollHeaderName;
  private long maxRecordsPerFile;
  private long maxFileSize;
  private WholeFileExistsAction wholeFileExistsAction;
  private String authTokenEndpoint;
  private String clientId;
  private String clientKey;
  private long idleTimeout;

  public DataLakeGeneratorManagerTestBuilder() {
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

  public DataLakeGeneratorManagerTestBuilder uniquePrefix(String uniquePrefix) {
    this.uniquePrefix = uniquePrefix;
    return this;
  }

  public DataLakeGeneratorManagerTestBuilder fileNameSuffix(String fileSuffix) {
    this.fileNameSuffix = fileSuffix;
    return this;
  }

  public DataLakeGeneratorManagerTestBuilder dirPathTemplateInHeader(boolean dirPathTemplateInHeader) {
    this.dirPathTemplateInHeader = dirPathTemplateInHeader;
    return this;
  }

  public DataLakeGeneratorManagerTestBuilder rollHeaderName(String rollHeaderName) {
    this.rollHeaderName = rollHeaderName;
    return this;
  }

  public DataLakeGeneratorManagerTestBuilder rollIfHeader(boolean rollIfHeader) {
    this.rollIfHeader = rollIfHeader;
    return this;
  }

  public DataLakeGeneratorManager build() {
    return new DataLakeGeneratorManager(
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
