/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.service.generator;

import com.streamsets.pipeline.api.base.BaseService;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.api.service.dataformats.SdcRecordGeneratorService;
import com.streamsets.pipeline.lib.generator.sdcrecord.SdcRecordDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

@ServiceDef(
  provides = SdcRecordGeneratorService.class,
  version = 1,
  upgrader = GeneratorUpgrader.class,
  label = "SDC_RECORD Generator"
)
public class SdcRecordGeneratorServiceImpl extends BaseService implements SdcRecordGeneratorService {

  private static final Logger LOG = LoggerFactory.getLogger(SdcRecordGeneratorServiceImpl.class);

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    ContextExtensions extensions = (ContextExtensions)getContext();
    return new DataGeneratorServiceImpl.DataGeneratorWraper(
      new SdcRecordDataGenerator(extensions.createRecordWriter(os), extensions)
    );
  }
}
