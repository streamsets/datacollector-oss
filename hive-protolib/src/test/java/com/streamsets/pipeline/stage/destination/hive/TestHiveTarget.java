/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hive;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class TestHiveTarget {
  // Disabled until there's a solution that doesn't require a HiveServer2 instance.
  private final String hiveUrl = "thrift://ip-172-31-1-81.us-west-2.compute.internal:9083";
  private final String schema = "default";
  private final String table = "alerts";

  @Test
  public void testWriteSingleRecord() throws Exception {
    TargetRunner targetRunner = new TargetRunner.Builder(HiveDTarget.class)
        .addConfiguration("hiveUrl", hiveUrl)
        .addConfiguration("schema", schema)
        .addConfiguration("table", table)
        .addConfiguration("txnBatchSize", 2)
        .addConfiguration("hiveConfDir", "/etc/hadoop/conf")
        .build();

    Record record = RecordCreator.create();
    Map<String, Field> kv1 = new HashMap<>();
    kv1.put("id", Field.create(1));
    kv1.put("msg", Field.create("Hello, streaming"));
    kv1.put("continent", Field.create("Asia"));
    kv1.put("country", Field.create("India"));
    record.set(Field.create(kv1));

    Record record2 = RecordCreator.create();
    Map<String, Field> kv2 = new HashMap<>();
    kv2.put("id", Field.create(2));
    kv2.put("msg", Field.create("Hello again, streaming"));
    kv2.put("continent", Field.create("Europe"));
    kv2.put("country", Field.create("Poland"));
    record.set(Field.create(kv2));

    List<Record> singleRecord = ImmutableList.of(record, record2);

    targetRunner.runValidateConfigs();
    targetRunner.runInit();
    targetRunner.runWrite(singleRecord);
    targetRunner.runDestroy();
  }
}
