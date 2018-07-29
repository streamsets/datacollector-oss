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

package com.streamsets.pipeline.stage.destination.pulsar;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import java.util.List;

public class PulsarTarget extends BaseTarget {

  private final PulsarTargetConfig pulsarConfig;
  private final PulsarMessageProducerFactory pulsarMessageProducerFactory;
  private PulsarMessageProducer pulsarMessageProducer;

  /**
   * Creates a PulsarTarget instance
   *
   * @param pulsarTargetConfig PulsarTarget configuration parameters. Cannot be null.
   * @param pulsarMessageProducerFactory Factory used to create PulsarMessageProducer instances. Cannot be null.
   */
  public PulsarTarget(
      PulsarTargetConfig pulsarTargetConfig,
      PulsarMessageProducerFactory pulsarMessageProducerFactory
  ) {

    this.pulsarConfig = Preconditions.checkNotNull(pulsarTargetConfig, "PulsarTargetConfig cannot be null");
    this.pulsarMessageProducerFactory = Preconditions.checkNotNull(pulsarMessageProducerFactory,
        "PulsarMessageProducerFactory cannot be null"
    );
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    //init pulsar message producer
    pulsarMessageProducer = pulsarMessageProducerFactory.create(pulsarConfig, getContext());
    issues.addAll(pulsarMessageProducer.init(getContext()));

    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    this.pulsarMessageProducer.put(batch);
  }

  @Override
  public void destroy() {
    if (this.pulsarMessageProducer != null) {
      this.pulsarMessageProducer.close();
    }

    super.destroy();
  }

}
