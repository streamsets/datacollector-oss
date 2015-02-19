/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.dedup;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ValueChooser;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.streamsets.pipeline.lib.queue.XEvictingQueue;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Record Deduplicator",
    description = "Separates unique and duplicate records based on field comparison",
    icon="dedup.svg",
    outputStreams = OutputStreams.class
)
@ConfigGroups(com.streamsets.pipeline.stage.processor.dedup.ConfigGroups.class)
public class DeDupProcessor extends RecordProcessor {

  private static final int MEMORY_USAGE_PER_HASH = 85;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "1000000",
      label = "Max Records to Compare",
      displayPosition = 10,
      group = "DE_DUP"
  )
  public int recordCountWindow;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "0",
      label = "Time to Compare (secs)",
      description = "Creates a window of time for comparison. Takes precedence over Max Records. Use 0 for no time window.",
      displayPosition = 20,
      group = "DE_DUP"
  )
  public int timeWindowSecs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ALL_FIELDS",
      label = "Compare",
      displayPosition = 30,
      group = "DE_DUP"
  )
  @ValueChooser(type = ChooserMode.PROVIDED, chooserValues = SelectFieldsChooserValues.class)
  public SelectFields compareFields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Fields to Compare",
      displayPosition = 40,
      group = "DE_DUP",
      dependsOn = "compareFields",
      triggeredByValue = "SPECIFIED_FIELDS"
  )
  @FieldSelector
  public List<String> fieldsToCompare;

  public static class RecordFunnel implements Funnel<Record> {
    private List<String> fieldsToHash;

    public RecordFunnel() {
    }

    public RecordFunnel(List<String> fieldsToHash) {
      this.fieldsToHash = fieldsToHash;
    }

    protected List<String> getFieldsToHash(Record record) {
      List<String> fields;
      if (fieldsToHash != null) {
        fields = fieldsToHash;
      } else {
        fields = new ArrayList<>(record.getFieldPaths());
        Collections.sort(fields);
      }
      return fields;
    }

    @Override
    public void funnel(Record record, PrimitiveSink sink) {
      for (String path : getFieldsToHash(record)) {
        Field field = record.get(path);
        if (field.getValue() != null) {
          switch (field.getType()) {
            case BOOLEAN:
              sink.putBoolean(field.getValueAsBoolean());
              break;
            case CHAR:
              sink.putChar(field.getValueAsChar());
              break;
            case BYTE:
              sink.putByte(field.getValueAsByte());
              break;
            case SHORT:
              sink.putShort(field.getValueAsShort());
              break;
            case INTEGER:
              sink.putInt(field.getValueAsInteger());
              break;
            case LONG:
              sink.putLong(field.getValueAsLong());
              break;
            case FLOAT:
              sink.putFloat(field.getValueAsFloat());
              break;
            case DOUBLE:
              sink.putDouble(field.getValueAsDouble());
              break;
            case DATE:
              sink.putLong(field.getValueAsDate().getTime());
              break;
            case DATETIME:
              sink.putLong(field.getValueAsDatetime().getTime());
              break;
            case DECIMAL:
              sink.putString(field.getValueAsString(), Charset.defaultCharset());
              break;
            case STRING:
              sink.putString(field.getValueAsString(), Charset.defaultCharset());
              break;
            case BYTE_ARRAY:
              sink.putBytes(field.getValueAsByteArray());
              break;
            case MAP:
            case LIST:
          }
        } else {
          sink.putBoolean(true);
        }
        sink.putByte((byte)0);
      }

    }
  }

  private static final Object VOID = new Object();

  private HashFunction hasher;
  private RecordFunnel funnel;
  private Cache<HashCode, Object> hashCache;
  private XEvictingQueue<HashCode> hashBuffer;
  private String uniqueLane;
  private String duplicateLane;

  private String hashAttrName;

  @Override
  protected List<ConfigIssue> validateConfigs() {
    List<ConfigIssue> issues = super.validateConfigs();
    if (recordCountWindow <= 0) {
      issues.add(getContext().createConfigIssue(Errors.DEDUP_00, recordCountWindow));
    }
    if (timeWindowSecs < 0) {
      issues.add(getContext().createConfigIssue(Errors.DEDUP_01, timeWindowSecs));
    }
    if (compareFields == SelectFields.SPECIFIED_FIELDS && fieldsToCompare.isEmpty()) {
      issues.add(getContext().createConfigIssue(Errors.DEDUP_02));
    }

    long estimatedMemory = MEMORY_USAGE_PER_HASH * recordCountWindow;
    if (estimatedMemory > Runtime.getRuntime().maxMemory() / 3) {
      issues.add(getContext().createConfigIssue(Errors.DEDUP_03, recordCountWindow, estimatedMemory / 1024,
                                                Runtime.getRuntime().maxMemory() / 1024));
    }
    return issues;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void init() throws StageException {
    super.init();
    hasher = Hashing.murmur3_128();
    funnel = (compareFields == SelectFields.ALL_FIELDS) ? new RecordFunnel() : new RecordFunnel(fieldsToCompare);
    CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
    if (timeWindowSecs > 0) {
      cacheBuilder.expireAfterWrite(timeWindowSecs, TimeUnit.SECONDS);
    }
    hashCache = cacheBuilder.build(new CacheLoader<HashCode, Object>() {
      @Override
      public Object load(HashCode key) throws Exception {
        return VOID;
      }
    });
    hashBuffer = XEvictingQueue.create(recordCountWindow);
    hashAttrName = getInfo() + ".hash";
    uniqueLane = getContext().getOutputLanes().get(OutputStreams.UNIQUE.ordinal());
    duplicateLane = getContext().getOutputLanes().get(OutputStreams.DUPLICATE.ordinal());
  }

  boolean duplicateCheck(Record record) {
    boolean dup = true;
    HashCode hash = hasher.hashObject(record, funnel);
    record.getHeader().setAttribute(hashAttrName, hash.toString());
    if (hashCache.getIfPresent(hash) == null) {
      hashCache.put(hash, VOID);
      HashCode evicted = hashBuffer.addAndGetEvicted(hash);
      if (evicted != null) {
        hashCache.invalidate(evicted);
      }
      dup = false;
    }
    return dup;
  }

  @Override
  protected void process(Record record, BatchMaker batchMaker) throws StageException {
    if (duplicateCheck(record)) {
      batchMaker.addRecord(record, duplicateLane);
    } else {
      batchMaker.addRecord(record, uniqueLane);
    }
  }

}
