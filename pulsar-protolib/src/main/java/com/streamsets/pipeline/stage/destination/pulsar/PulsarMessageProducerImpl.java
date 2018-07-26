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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.api.service.dataformats.DataGeneratorException;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.pulsar.config.PulsarErrors;
import com.streamsets.pipeline.lib.pulsar.config.PulsarGroups;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.FlusherProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PulsarMessageProducerImpl implements PulsarMessageProducer {

  private static final Logger LOG = LoggerFactory.getLogger(PulsarMessageProducerImpl.class);
  private static final String PULSAR_TARGET_CONFIG_SERVICE_URL = "pulsarTargetConfig.serviceURL";

  private final PulsarTargetConfig pulsarTargetConfig;
  private final Stage.Context context;
  private final DataFormatGeneratorService dataFormatGeneratorService;
  private ELEval destinationEval;
  private ErrorRecordHandler errorHandler;
  private PulsarClient pulsarClient;
  private LoadingCache<String, Producer> messageProducers;


  public PulsarMessageProducerImpl(PulsarTargetConfig pulsarTargetConfig, Stage.Context context) {
    this.pulsarTargetConfig = Preconditions.checkNotNull(pulsarTargetConfig);
    this.context = Preconditions.checkNotNull(context);
    this.dataFormatGeneratorService = Preconditions.checkNotNull(context.getService(DataFormatGeneratorService.class));
  }

  public PulsarClient getPulsarClient() {
    return pulsarClient;
  }

  public void setPulsarClient(PulsarClient pulsarClient) {
    this.pulsarClient = pulsarClient;
  }

  public LoadingCache<String, Producer> getMessageProducers() {
    return messageProducers;
  }

  public void setMessageProducers(LoadingCache<String, Producer> messageProducers) {
    this.messageProducers = messageProducers;
  }

  @Override
  public List<Stage.ConfigIssue> init(Target.Context context) {
    List<Stage.ConfigIssue> issues = new ArrayList<>();
    // pulsar client
    try {
      pulsarClient = PulsarClient.builder().serviceUrl(pulsarTargetConfig.serviceURL).keepAliveInterval(
          pulsarTargetConfig.keepAliveInterval,
          TimeUnit.SECONDS
      ).operationTimeout(pulsarTargetConfig.operationTimeout, TimeUnit.SECONDS).build();
    } catch (PulsarClientException | IllegalArgumentException e) {
      LOG.debug(Utils.format(PulsarErrors.PULSAR_00.getMessage(), pulsarTargetConfig.serviceURL, e.toString()), e);
      issues.add(context.createConfigIssue(PulsarGroups.PULSAR.name(),
          PULSAR_TARGET_CONFIG_SERVICE_URL,
          PulsarErrors.PULSAR_00,
          pulsarTargetConfig.serviceURL,
          e.toString()
      ));
    }

    // pulsar message producers
    messageProducers = CacheBuilder.newBuilder()
                                   .expireAfterAccess(15, TimeUnit.MINUTES)
                                   .removalListener((RemovalListener<String, Producer>) removalNotification -> {
                                     try {
                                       removalNotification.getValue().close();
                                     } catch (PulsarClientException e) {
                                       LOG.warn("Exception when trying to remove pulsar message producer {}. " +
                                           "Exception: {}", removalNotification.getValue().getProducerName(), e);
                                     }
                                   })
                                   .build(new CacheLoader<String, Producer>() {
                                     @Override
                                     public Producer load(String key) throws Exception {
                                       // TODO remove FlusherProducer wrapper, SDC-9554
                                       return new FlusherProducer<>(pulsarClient.newProducer().topic(key).create());
                                     }
                                   });

    destinationEval = context.createELEval("destinationName");
    errorHandler = new DefaultErrorRecordHandler(context);

    return issues;
  }

  @Override
  public void put(Batch batch) throws StageException {
    String destinationName = "Unknown";
    Set<Producer> usedProducers = new HashSet<>();

    if (batch != null) {
      Iterator<Record> recordIterator = batch.getRecords();
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

      while (recordIterator.hasNext()) {
        byteArrayOutputStream.reset();
        Record record = recordIterator.next();

        try {
          try (DataGenerator generator = dataFormatGeneratorService.getGenerator(byteArrayOutputStream)) {
            generator.write(record);
          } catch (DataGeneratorException e) {
            handleError(record, e);
          } catch (IOException e) {
            throw new StageException(PulsarErrors.PULSAR_01, e.getMessage(), e);
          }

          // Resolve destination
          ELVars elVars = context.createELVars();
          RecordEL.setRecordInContext(elVars, record);
          destinationName = destinationEval.eval(elVars, pulsarTargetConfig.destinationName, String.class);

          // Send message
          Producer producer = messageProducers.get(destinationName);
          producer.send(byteArrayOutputStream.toByteArray());
          usedProducers.add(producer);
        } catch (PulsarClientException e) {
          handleError(record, new StageException(PulsarErrors.PULSAR_04, destinationName, e.getMessage(), e));
        } catch (ExecutionException e) {
          handleError(record, new StageException(PulsarErrors.PULSAR_03, destinationName, e.getMessage(), e));
        } catch (ELEvalException e) {
          handleError(record, new StageException(PulsarErrors.PULSAR_02, e.getMessage(), e));
        }
      }

      for (Producer producer : usedProducers) {
        // TODO remove cast, SDC-9554
        ((FlusherProducer)producer).flush();
      }
    }
  }

  private void handleError(Record record, StageException stageException) throws StageException {
    errorHandler.onError(new OnRecordErrorException(record, stageException.getErrorCode(), stageException.getParams()));
  }

  @Override
  public void close() {
    try {
      if (pulsarClient != null) {
        pulsarClient.close(); // also closes producers and consumers used by it
      }
    } catch (PulsarClientException e) {
      LOG.warn("Cloud not close Pulsar client: {}", e);
    }
  }

}
