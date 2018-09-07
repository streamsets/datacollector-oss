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

package com.streamsets.pipeline.hbase.api;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.hbase.api.common.processor.HBaseLookupConfig;
import com.streamsets.pipeline.hbase.api.common.producer.HBaseConnectionConfig;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

public class HBaseFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFactory.class);

  private static ServiceLoader<HBaseProvider>
      hBaseProducerProviderServiceLoader
      = ServiceLoader.load(HBaseProvider.class);

  private static HBaseProvider hBaseProvider;

  static {
    int serviceCount = 0;
    for (HBaseProvider bean : hBaseProducerProviderServiceLoader) {
      LOG.info("Found FactoriesBean loader {}", bean.getClass().getName());
      hBaseProvider = bean;
      serviceCount++;
    }

    if (serviceCount != 1) {
      throw new RuntimeException(Utils.format("Unexpected number of FactoriesBean: {} instead of 1", serviceCount));
    }
  }

  public static HBaseProducer createProducer(
      Stage.Context context, HBaseConnectionConfig conf, ErrorRecordHandler errorRecordHandler
  ) {
    return hBaseProvider.createProducer(context, conf, errorRecordHandler);
  }

  public static HBaseProcessor createProcessor(
      Stage.Context context, HBaseLookupConfig conf, ErrorRecordHandler errorRecordHandler
  ) {
    return hBaseProvider.createProcessor(context, conf, errorRecordHandler);
  }
}
