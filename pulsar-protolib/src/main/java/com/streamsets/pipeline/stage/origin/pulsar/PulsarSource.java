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

package com.streamsets.pipeline.stage.origin.pulsar;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.Errors;
import com.streamsets.pipeline.lib.pulsar.config.PulsarErrors;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PulsarSource extends BaseSource implements OffsetCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(PulsarSource.class);

  private final BasicConfig basicConfig;
  private final PulsarSourceConfig pulsarConfig;
  private final PulsarMessageConsumerFactory pulsarMessageConsumerFactory;
  private final PulsarMessageConverter pulsarMessageConverter;
  private PulsarMessageConsumer pulsarMessageConsumer;
  private boolean checkBatchSize = true;

  public PulsarSource(
      BasicConfig basicConfig,
      PulsarSourceConfig pulsarSourceConfig,
      PulsarMessageConsumerFactory pulsarMessageConsumerFactory,
      PulsarMessageConverter pulsarMessageConverter
  ) {
    this.basicConfig = Preconditions.checkNotNull(basicConfig, "Basicconfig cannot be null");
    this.pulsarConfig = Preconditions.checkNotNull(pulsarSourceConfig, "pulsarSourceConfig cannot be null");
    this.pulsarMessageConsumerFactory = Preconditions.checkNotNull(pulsarMessageConsumerFactory,
        "PulsarMessageConsumerFactory cannot be null"
    );
    this.pulsarMessageConverter = Preconditions.checkNotNull(pulsarMessageConverter,
        "PulsarMessageConverter cannot be null"
    );
  }

  public void setPulsarMessageConsumer(PulsarMessageConsumer pulsarMessageConsumer) {
    this.pulsarMessageConsumer = pulsarMessageConsumer;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    //init pulsar message converter
    issues.addAll(pulsarMessageConverter.init(getContext()));

    //init pulsar message producer
    pulsarMessageConsumer = pulsarMessageConsumerFactory.create(
        basicConfig,
        pulsarConfig,
        pulsarMessageConverter
    );
    issues.addAll(pulsarMessageConsumer.init(getContext()));

    return issues;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int batchSize = Math.min(basicConfig.maxBatchSize, maxBatchSize);
    if (!getContext().isPreview() && checkBatchSize && basicConfig.maxBatchSize > maxBatchSize) {
      getContext().reportError(PulsarErrors.PULSAR_18, maxBatchSize);
      checkBatchSize = false;
    }

    pulsarMessageConsumer.take(batchMaker, getContext(), batchSize);
    return "";

  }

  @Override
  public void commit(String offset) throws StageException {
    pulsarMessageConsumer.ack();
  }

  @Override
  public void destroy() {
    if (this.pulsarMessageConsumer != null) {
      this.pulsarMessageConsumer.close();
    }

    super.destroy();
  }

}
