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
package com.streamsets.pipeline.stage.destination.jms;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.common.CredentialsConfig;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;

public class JmsMessageProducerFactoryImpl implements JmsMessageProducerFactory {

  @Override
  public JmsMessageProducer create(
      InitialContext initialContext,
      ConnectionFactory connectionFactory,
      DataFormat dataFormat,
      DataGeneratorFormatConfig dataFormatConfig,
      CredentialsConfig credentialsConfig,
      JmsTargetConfig jmsTargetConfig,
      Stage.Context context
  ) {
    return new JmsMessageProducerImpl(
        initialContext,
        connectionFactory,
        dataFormat,
        dataFormatConfig,
        credentialsConfig,
        jmsTargetConfig,
        context
    );
  }
}
