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
package com.streamsets.pipeline.sdk.service;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseService;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.api.service.dataformats.WholeFileChecksumAlgorithm;
import com.streamsets.pipeline.api.service.dataformats.WholeFileExistsAction;

import java.io.IOException;
import java.io.OutputStream;

@ServiceDef(
  provides = DataFormatGeneratorService.class,
  version = 1,
  label = "(Test) Runner implementation of very simple DataFormatGeneratorService that will always work with whole file format."
)
public class SdkWholeFileDataFormatGeneratorService extends BaseService implements DataFormatGeneratorService {

  private String wholeFileNamePath;
  private WholeFileExistsAction existsAction;
  private boolean includeChecksumInTheEvents;
  private WholeFileChecksumAlgorithm checksumAlgorithm;

  public SdkWholeFileDataFormatGeneratorService() {
    this("/fileName", WholeFileExistsAction.OVERWRITE, false, WholeFileChecksumAlgorithm.MD5);
  }

  public SdkWholeFileDataFormatGeneratorService(
    String wholeFileNamePath,
    WholeFileExistsAction existsAction,
    boolean includeChecksumInTheEvents,
    WholeFileChecksumAlgorithm checksumAlgorithm
  ) {
    this.wholeFileNamePath = wholeFileNamePath;
    this.existsAction = existsAction;
    this.includeChecksumInTheEvents = includeChecksumInTheEvents;
    this.checksumAlgorithm = checksumAlgorithm;
  }


  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    throw new UnsupportedOperationException("Only Whole File Format is supported");
  }

  @Override
  public boolean isPlainTextCompatible() {
    throw new UnsupportedOperationException("Only Whole File Format is supported");
  }

  @Override
  public String getCharset() {
    throw new UnsupportedOperationException("Only Whole File Format is supported");
  }

  @Override
  public boolean isWholeFileFormat() {
    return true;
  }

  @Override
  public String wholeFileFilename(Record record) throws StageException {
    return record.get(wholeFileNamePath).getValueAsString();
  }

  @Override
  public WholeFileExistsAction wholeFileExistsAction() {
    return existsAction;
  }

  @Override
  public boolean wholeFileIncludeChecksumInTheEvents() {
    return includeChecksumInTheEvents;
  }

  @Override
  public WholeFileChecksumAlgorithm wholeFileChecksumAlgorithm() {
    return checksumAlgorithm;
  }
}
