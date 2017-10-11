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
package com.streamsets.pipeline.stage.devtest;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@GenerateResourceBundle
@StageDef(
  version = 5,
  label="Dev Data Generator",
  description = "Generates records with the specified field names based on the selected data type. For development only.",
  execution = ExecutionMode.STANDALONE,
  icon= "dev.png",
  producesEvents = true,
  upgrader = RandomDataGeneratorSourceUpgrader.class,
  onlineHelpRefUrl = "index.html#Pipeline_Design/DevStages.html"
)
public class RandomDataGeneratorSource extends BasePushSource {

  private static final Logger LOG = LoggerFactory.getLogger(RandomDataGeneratorSource.class);

  private final int EVENT_VERSION = 1;

  private final List<String> tzValues = new TimeZoneChooserValues().getValues();

  private final Random random = new Random();

  @ConfigDef(label = "Fields to Generate", required = false, type = ConfigDef.Type.MODEL, defaultValue="",
    description="Fields to generate of the indicated type")
  @ListBeanModel
  public List<DataGeneratorConfig> dataGenConfigs;

  @ConfigDef(label = "Root Field Type",
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "MAP",
    description = "Field Type for root object")
  @ValueChooserModel(RootTypeChooserValueProvider.class)
  public RootType rootFieldType;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.MAP,
    label = "Header Attributes",
    description = "Attributes to be put in the generated record header"
  )
  public Map<String, String> headerAttributes;

  @ConfigDef(required = true, type = ConfigDef.Type.NUMBER,
    defaultValue = "1000",
    label = "Delay Between Batches",
    description = "Milliseconds to wait before sending the next batch",
    min = 0,
    max = Integer.MAX_VALUE)
  public int delay;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "1000",
    label = "Batch size",
    description = "Number of records that will be generated for single batch.",
    min = 1,
    max = Integer.MAX_VALUE)
  public int batchSize;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.NUMBER,
    defaultValue = "1",
    label = "Number of Threads",
    description = "Number of concurrent threads that will be generating data in parallel.",
    min = 1,
    max = Integer.MAX_VALUE)
  public int numThreads;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue = "generated-event",
    label = "Event name",
    description = "Name of event that should be used when generating events."
  )
  public String eventName;

  /**
   * Counter for LONG_SEQUENCE type
   */
  private long counter;

  /**
   * Max batch size the origin should produce.
   */
  private int maxBatchSize;

  @Override
  protected List<ConfigIssue> init() {
    counter = 0;

    LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_READ);
    event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.DEVDATA.name());
    event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, getContext().getStageInfo().getName());
    List<String> names = new ArrayList<>();
    for(DataGeneratorConfig con : dataGenConfigs) {
      names.add(con.field.isEmpty() ? "<empty field name>" : con.field);
    }
    event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, names.isEmpty() ? "No fields" : StringUtils.join(names, ", "));
    getContext().publishLineageEvent(event);

    return super.init();
  }

  @Override
  public int getNumberOfThreads() {
    return numThreads;
  }

  @Override
  public void produce(Map<String, String> offsets, int maxBatchSize) throws StageException {
    this.maxBatchSize = Math.min(maxBatchSize, batchSize);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<Runnable>> futures = new ArrayList<>(numThreads);

    // Run all the threads
    for(int i = 0; i < numThreads; i++) {
      Future future = executor.submit(new GeneratorRunnable(i));
      futures.add(future);
    }

    // Wait for proper execution finish
    for(Future<Runnable> f : futures) {
      try {
        f.get();
      } catch (InterruptedException|ExecutionException e) {
        LOG.error("Interrupted data generation thread", e);
      }
    }

    // Terminate executor that will also clear up threads that were created
    LOG.info("Shutting down executor service");
    executor.shutdownNow();
  }

  public class GeneratorRunnable implements Runnable {
    int threadId;

    GeneratorRunnable(int threadId) {
      this.threadId = threadId;
    }

    @Override
    public void run() {
      // Override thread name, so that it's easier to find threads from this origin
      Thread.currentThread().setName("RandomDataGenerator-" + threadId);

      while (!getContext().isStopped()) {
        LOG.trace("Starting new batch in thread {}", threadId);

        // Create new batch
        BatchContext batchContext = getContext().startBatch();

        // Fill it with random records
        for (int i = 0; i < maxBatchSize; i++) {
          createRecord(threadId, i, batchContext);
        }

        // And finally send them the rest of the pipeline for further processing
        getContext().processBatch(batchContext);

        // Wait if configured
        if (delay > 0 && !getContext().isPreview()) {
          ThreadUtil.sleep(delay);
        }
      }
    }
  }

  private void createRecord(int threadId, int batchOffset, BatchContext batchContext) {
    // Generate random data per configuration
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    for(DataGeneratorConfig dataGeneratorConfig : dataGenConfigs) {
      map.put(dataGeneratorConfig.field, generateRandomData(dataGeneratorConfig));
    }

    // Sent normal record
    Record record = getContext().createRecord("random:" + threadId + ":" + batchOffset);
    fillRecord(record, map);
    batchContext.getBatchMaker().addRecord(record);

    String recordSourceId = Utils.format("event:{}:{}:{}", eventName, EVENT_VERSION, batchOffset);
    EventRecord event = getContext().createEventRecord(eventName, EVENT_VERSION, recordSourceId);
    fillRecord(event, map);
    batchContext.toEvent(event);
  }

  private void fillRecord(Record record, LinkedHashMap map) {
    // Fill header
    if(headerAttributes != null && !headerAttributes.isEmpty()) {
      for (Map.Entry<String, String> e : headerAttributes.entrySet()) {
        record.getHeader().setAttribute(e.getKey(), e.getValue());
      }
    }

    // Fill Data
    switch (rootFieldType) {
      case MAP:
        record.set(Field.create(map));
        break;
      case LIST_MAP:
        record.set(Field.createListMap(map));
        break;
    }
  }

  private Field generateRandomData(DataGeneratorConfig config) {
    switch(config.type) {
      case BOOLEAN :
        return Field.create(Field.Type.BOOLEAN, random.nextBoolean());
      case DATE:
        return Field.create(Field.Type.DATE, getRandomDate());
      case DATETIME:
        return Field.create(Field.Type.DATETIME, getRandomDateTime());
      case TIME:
        return Field.create(Field.Type.TIME, getRandomTime());
      case DOUBLE:
        return Field.create(Field.Type.DOUBLE, random.nextDouble());
      case FLOAT:
        return Field.create(Field.Type.FLOAT, random.nextFloat());
      case INTEGER:
        return Field.create(Field.Type.INTEGER, random.nextInt());
      case LONG:
        return Field.create(Field.Type.LONG, random.nextLong());
      case STRING:
        return Field.create(Field.Type.STRING, UUID.randomUUID().toString());
      case DECIMAL:
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(random.nextLong() % (long)Math.pow(10, config.precision)), config.scale);
        Field decimalField = Field.create(Field.Type.DECIMAL, decimal);
        decimalField.setAttribute(HeaderAttributeConstants.ATTR_SCALE, String.valueOf(config.scale));
        decimalField.setAttribute(HeaderAttributeConstants.ATTR_PRECISION, String.valueOf(config.precision));
        return decimalField;
      case BYTE_ARRAY:
        return Field.create(Field.Type.BYTE_ARRAY, "StreamSets Inc, San Francisco".getBytes(StandardCharsets.UTF_8));
      case LONG_SEQUENCE:
        return Field.create(Field.Type.LONG, counter++);
      case ZONED_DATETIME:
        return Field.create(Field.Type.ZONED_DATETIME, getRandomZonedDateTime());
    }
    return null;
  }

  public Date getRandomDate() {
    GregorianCalendar gc = new GregorianCalendar();
    gc.set(
      randBetween(1990, 2020),
      randBetween(1, gc.getActualMaximum(gc.MONTH)),
      randBetween(1, gc.getActualMaximum(gc.DAY_OF_MONTH)),
      0, 0, 0
    );
    return gc.getTime();
  }

  public Date getRandomTime() {
    GregorianCalendar gc = new GregorianCalendar();
    gc.set(
      1970, 0, 1,
      randBetween(0, gc.getActualMaximum(gc.HOUR_OF_DAY)),
      randBetween(0, gc.getActualMaximum(gc.MINUTE)),
      randBetween(0, gc.getActualMaximum(gc.SECOND))
    );
    return gc.getTime();
  }

  public Date getRandomDateTime() {
    GregorianCalendar gc = new GregorianCalendar();
    gc.set(
      randBetween(1990, 2020),
      randBetween(1, gc.getActualMaximum(gc.MONTH)),
      randBetween(1, gc.getActualMaximum(gc.DAY_OF_MONTH)),
      randBetween(0, gc.getActualMaximum(gc.HOUR_OF_DAY)),
      randBetween(0, gc.getActualMaximum(gc.MINUTE)),
      randBetween(0, gc.getActualMaximum(gc.SECOND))
    );
    return gc.getTime();
  }

  public ZonedDateTime getRandomZonedDateTime() {
    String zoneId = tzValues.get(randBetween(0, tzValues.size()));
    return ZonedDateTime.of(
        randBetween(1990, 2020),
        randBetween(1, 12),
        randBetween(1, 28),
        randBetween(0, 23),
        randBetween(0, 60),
        0,
        0,
        ZoneId.of(zoneId));
  }

  public static int randBetween(int start, int end) {
    return start + (int)Math.round(Math.random() * (end - start));
  }

  public static class DataGeneratorConfig {

    @ConfigDef(required = true, type = ConfigDef.Type.STRING,
      label = "Field Name")
    public String field;

    @ConfigDef(required = true, type = ConfigDef.Type.MODEL,
      label = "Field Type",
      defaultValue = "STRING")
    @ValueChooserModel(TypeChooserValueProvider.class)
    public Type type;

    @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "10",
      label = "Precision",
      description = "Precision of the generated decimal.",
      min = 0,
      dependsOn = "type",
      triggeredByValue = "DECIMAL"
    )
    public long precision;

    @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "2",
      label = "scale",
      description = "Scale of the generated decimal.",
      min = 0,
      dependsOn = "type",
      triggeredByValue = "DECIMAL"
    )
    public int scale;

  }

  enum Type {
    STRING,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE,
    DATETIME,
    ZONED_DATETIME,
    TIME,
    BOOLEAN,
    DECIMAL,
    BYTE_ARRAY,
    LONG_SEQUENCE
  }

  enum RootType {
    MAP,
    LIST_MAP
  }

}
