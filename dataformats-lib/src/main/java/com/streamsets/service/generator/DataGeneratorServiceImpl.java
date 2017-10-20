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
package com.streamsets.service.generator;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.BaseService;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

@ServiceDef(
  provides = DataFormatGeneratorService.class,
  version = 1,
  label = "DataFormat Generator"
)
@ConfigGroups(Groups.class)
public class DataGeneratorServiceImpl extends BaseService implements DataFormatGeneratorService {

  private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorServiceImpl.class);

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Data Format",
    description = "Data Format",
    displayPosition = 1,
    group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDefBean()
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  @Override
  public List<ConfigIssue> init() {
    LOG.debug("Initializing dataformat service.");
    /* TODO: We need to first abstract some of the common interfaces up
    return dataGeneratorFormatConfig.init(
      context,
      dataFormat,
      "DATA_FORMAT",
      "dataGeneratorFormatConfig.",
      new LinkedList<>()
    );
    */

    return Collections.emptyList();
  }

  @Override
  public void destroy() {
    LOG.debug("Destroying DataFormat service.");
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    return null;
  }
}
