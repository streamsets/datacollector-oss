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

import com.codahale.metrics.Timer;
import com.github.javafaker.Faker;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
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
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
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
    execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
    icon= "dev.png",
    producesEvents = true,
    recordsByRef = true,
    upgrader = RandomDataGeneratorSourceUpgrader.class,
    upgraderDef = "upgrader/RandomDataGeneratorSource.yaml",
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Pipeline_Design/DevStages.html"
)
@ConfigGroups(value = RandomDataGeneratorGroups.class)
public class RandomDataGeneratorSource extends BasePushSource {

  private static final Logger LOG = LoggerFactory.getLogger(RandomDataGeneratorSource.class);

  private final int EVENT_VERSION = 1;

  private final ThreadLocal<Faker> faker = ThreadLocal.withInitial(Faker::new);

  private final List<String> tzValues = new ArrayList<>(ZoneId.getAvailableZoneIds());

  private final Random random = new Random();

  @ConfigDef(
      label = "Fields to Generate",
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      description="Fields to generate of the indicated type",
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_GENERATOR"
  )
  @ListBeanModel
  public List<DataGeneratorConfig> dataGenConfigs;

  @ConfigDef(
      label = "Root Field Type",
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "MAP",
      description = "Field Type for root object",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DATA_GENERATOR"
  )
  @ValueChooserModel(RootTypeChooserValueProvider.class)
  public RootType rootFieldType;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MAP,
      label = "Header Attributes",
      description = "Attributes to be put in the generated record header",
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "DATA_GENERATOR"
  )
  public Map<String, String> headerAttributes;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Delay Between Batches",
      description = "Milliseconds to wait before sending the next batch",
      min = 0,
      max = Integer.MAX_VALUE,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DATA_GENERATOR"
  )
  public int delay;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Batch size",
      description = "Number of records that will be generated for single batch.",
      min = 1,
      max = Integer.MAX_VALUE,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DATA_GENERATOR"
  )
  public int batchSize;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1",
      label = "Number of Threads",
      description = "Number of concurrent threads that will be generating data in parallel.",
      min = 1,
      max = Integer.MAX_VALUE,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DATA_GENERATOR"
  )
  public int numThreads;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "generated-event",
      label = "Event name",
      description = "Name of event that should be used when generating events.",
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "DATA_GENERATOR"
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

  private Timer dataGeneratorTimer;

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
    event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, names.isEmpty() ? "No fields" : String.join(", ", names));
    getContext().publishLineageEvent(event);

    this.dataGeneratorTimer = getContext().createTimer("Data Generator");

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

    StageException propagateException = null;

    try {
      // Run all the threads
      for (int i = 0; i < numThreads; i++) {
        Future future = executor.submit(new GeneratorRunnable(i));
        futures.add(future);
      }

      // Wait for proper execution finish
      for (Future<Runnable> f : futures) {
        try {
          f.get();
        } catch (InterruptedException | ExecutionException e) {
          LOG.error("Interrupted data generation thread", e);
          if(propagateException == null) {
            propagateException = new StageException(Errors.DEV_001, e.toString(), e);
          }
        }
      }
    } finally {
      // Terminate executor that will also clear up threads that were created
      LOG.info("Shutting down executor service");
      executor.shutdownNow();
    }

    if(propagateException != null) {
      throw propagateException;
    }
  }

  public class GeneratorRunnable implements Runnable {
    int threadId;

    GeneratorRunnable(int threadId) {
      this.threadId = threadId;
    }

    @Override
    public void run() {
      // Override thread name, so that it's easier to find threads from this origin
      Thread.currentThread().setName("RandomDataGenerator-" + threadId + "::" + getContext().getPipelineId());

      while (!getContext().isStopped()) {
        LOG.trace("Starting new batch in thread {}", threadId);

        // Create new batch
        BatchContext batchContext = getContext().startBatch();

        // Fill it with random records
        Timer.Context tc  = dataGeneratorTimer.time();
        for (int i = 0; i < maxBatchSize; i++) {
          createRecord(threadId, i, batchContext);
        }
        tc.stop();

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

      case ADDRESS_FULL_ADDRESS:
        return Field.create(faker.get().address().fullAddress());

      case ADDRESS_BUILDING_NUMBER:
        return Field.create(faker.get().address().buildingNumber());

      case ADDRESS_STREET_ADDRESS:
        return Field.create(faker.get().address().streetAddress());

      case ADDRESS_CITY:
        return Field.create(faker.get().address().city());

      case ADDRESS_STATE:
        return Field.create(faker.get().address().state());

      case ADDRESS_COUNTRY:
        return Field.create(faker.get().address().country());

      case ADDRESS_LATITUDE:
        return Field.create(faker.get().address().latitude());

      case ADDRESS_LONGITUDE:
        return Field.create(faker.get().address().longitude());

      case APP_NAME:
        return Field.create(faker.get().app().name());

      case APP_AUTHOR:
        return Field.create(faker.get().app().author());

      case APP_VERSION:
        return Field.create(faker.get().app().version());

      case ARTIST_NAME:
        return Field.create(faker.get().artist().name());

      case BOOK_TITLE:
        return Field.create(faker.get().book().title());

      case BOOK_AUTHOR:
        return Field.create(faker.get().book().author());

      case BOOK_GENRE:
        return Field.create(faker.get().book().genre());

      case BOOK_PUBLISHER:
        return Field.create(faker.get().book().publisher());

      case BUSINESS_CREDIT_CARD_NUMBER:
        return Field.create(faker.get().business().creditCardNumber());

      case BUSINESS_CREDIT_CARD_EXPIRY:
        return Field.create(faker.get().business().creditCardExpiry());

      case BUSINESS_CREDIT_CARD_TYPE:
        return Field.create(faker.get().business().creditCardType());

      case CAT_NAME:
        return Field.create(faker.get().cat().name());

      case CAT_BREED:
        return Field.create(faker.get().cat().breed());

      case CAT_REGISTRY:
        return Field.create(faker.get().cat().registry());

      case CODE_ASIN:
        return Field.create(faker.get().code().asin());

      case CODE_IMEI:
        return Field.create(faker.get().code().imei());

      case CODE_ISBN10:
        return Field.create(faker.get().code().isbn10());

      case CODE_ISBN13:
        return Field.create(faker.get().code().isbn13());

      case COLOR:
        return Field.create(faker.get().color().name());

      case COMMERCE_DEPARTMENT:
        return Field.create(faker.get().commerce().department());

      case COMMERCE_COLOR:
        return Field.create(faker.get().commerce().color());

      case COMMERCE_MATERIAL:
        return Field.create(faker.get().commerce().material());

      case COMMERCE_PRICE:
        return Field.create(faker.get().commerce().price());

      case COMMERCE_PRODUCT_NAME:
        return Field.create(faker.get().commerce().productName());

      case COMMERCE_PROMOTION_CODE:
        return Field.create(faker.get().commerce().promotionCode());

      case COMPANY_NAME:
        return Field.create(faker.get().company().name());

      case COMPANY_INDUSTRY:
        return Field.create(faker.get().company().industry());

      case COMPANY_BUZZWORD:
        return Field.create(faker.get().company().buzzword());

      case COMPANY_URL:
        return Field.create(faker.get().company().url());

      case CRYPTO_MD5:
        return Field.create(faker.get().crypto().md5());

      case CRYPTO_SHA1:
        return Field.create(faker.get().crypto().sha1());

      case CRYPTO_SHA256:
        return Field.create(faker.get().crypto().sha256());

      case CRYPTO_SHA512:
        return Field.create(faker.get().crypto().sha512());

      case DEMOGRAPHIC:
        return Field.create(faker.get().demographic().race());

      case EDUCATOR:
        return Field.create(faker.get().educator().university());

      case EMAIL:
        return Field.create(faker.get().internet().emailAddress());

      case FILE:
        return Field.create(faker.get().file().fileName());

      case FINANCE:
        return Field.create(faker.get().finance().creditCard());

      case FOOD:
        return Field.create(faker.get().food().ingredient());

      case GAMEOFTHRONES:
        return Field.create(faker.get().gameOfThrones().character());

      case HACKER:
        return Field.create(faker.get().hacker().abbreviation());

      case IDNUMBER:
        return Field.create(faker.get().idNumber().valid());

      case INTERNET:
        return Field.create(faker.get().internet().domainName());

      case LOREM:
        return Field.create(faker.get().lorem().character());

      case MUSIC:
        return Field.create(faker.get().music().chord());

      case NAME:
        return Field.create(faker.get().name().fullName());

      case PHONENUMBER:
        return Field.create(faker.get().phoneNumber().cellPhone());

      case POKEMON:
        return Field.create(faker.get().pokemon().name());

      case RACE:
        return Field.create(faker.get().demographic().race());

      case SEX:
        return Field.create(faker.get().demographic().sex());

      case SHAKESPEARE:
        return Field.create(faker.get().shakespeare().romeoAndJulietQuote());

      case SLACKEMOJI:
        return Field.create(faker.get().slackEmoji().celebration());

      case SPACE:
        return Field.create(faker.get().space().company());

      case SSN:
        return Field.create(faker.get().idNumber().ssnValid());

      case STOCK:
        return Field.create(faker.get().stock().nsdqSymbol());

      case SUPERHERO:
        return Field.create(faker.get().superhero().name());

      case TEAM:
        return Field.create(faker.get().team().name());

      case UNIVERSITY:
        return Field.create(faker.get().university().name());
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
    String zoneId = tzValues.get(randBetween(0, tzValues.size() - 1));
    return ZonedDateTime.of(
        randBetween(1990, 2020),
        randBetween(1, 12),
        randBetween(1, 28),
        randBetween(0, 23),
        randBetween(0, 59),
        0,
        0,
        ZoneId.of(zoneId));
  }

  public static int randBetween(int start, int end) {
    return start + (int)Math.round(Math.random() * (end - start));
  }

  public static class DataGeneratorConfig {

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.STRING,
        label = "Field Name",
        displayMode = ConfigDef.DisplayMode.BASIC
    )
    public String field;

    @ConfigDef(
        required = true,
        type = ConfigDef.Type.MODEL,
        label = "Field Type",
        defaultValue = "STRING",
        displayMode = ConfigDef.DisplayMode.BASIC
    )
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
        displayMode = ConfigDef.DisplayMode.BASIC,
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
        displayMode = ConfigDef.DisplayMode.BASIC,
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
    LONG_SEQUENCE,

    // FAKER Types
    ADDRESS_FULL_ADDRESS,
    ADDRESS_BUILDING_NUMBER,
    ADDRESS_STREET_ADDRESS,
    ADDRESS_CITY,
    ADDRESS_STATE,
    ADDRESS_COUNTRY,
    ADDRESS_LATITUDE,
    ADDRESS_LONGITUDE,

    APP_NAME,
    APP_AUTHOR,
    APP_VERSION,

    ARTIST_NAME,

    BOOK_TITLE,
    BOOK_AUTHOR,
    BOOK_GENRE,
    BOOK_PUBLISHER,

    BUSINESS_CREDIT_CARD_NUMBER,
    BUSINESS_CREDIT_CARD_EXPIRY,
    BUSINESS_CREDIT_CARD_TYPE,

    CAT_NAME,
    CAT_BREED,
    CAT_REGISTRY,

    CODE_ASIN,
    CODE_IMEI,
    CODE_ISBN10,
    CODE_ISBN13,

    COLOR,

    COMMERCE_DEPARTMENT,
    COMMERCE_COLOR,
    COMMERCE_MATERIAL,
    COMMERCE_PRICE,
    COMMERCE_PRODUCT_NAME,
    COMMERCE_PROMOTION_CODE,

    COMPANY_NAME,
    COMPANY_INDUSTRY,
    COMPANY_BUZZWORD,
    COMPANY_URL,

    CRYPTO_MD5,
    CRYPTO_SHA1,
    CRYPTO_SHA256,
    CRYPTO_SHA512,

    DEMOGRAPHIC,
    EDUCATOR,
    EMAIL,
    FILE,
    FINANCE,
    FOOD,
    GAMEOFTHRONES,
    HACKER,
    IDNUMBER,
    INTERNET,
    LOREM,
    MUSIC,
    NAME,
    PHONENUMBER,
    POKEMON,
    RACE,
    SEX,
    SHAKESPEARE,
    SLACKEMOJI,
    SPACE,
    SSN,
    STOCK,
    SUPERHERO,
    TEAM,
    UNIVERSITY
  }

  enum RootType {
    MAP,
    LIST_MAP
  }

}
