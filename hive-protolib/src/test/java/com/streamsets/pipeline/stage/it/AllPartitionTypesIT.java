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
package com.streamsets.pipeline.stage.it;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.PartitionConfigBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.processor.hive.Errors;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import com.streamsets.pipeline.stage.processor.hive.PartitionConfig;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Run all various data types that are available for partitioning column.
 */
@RunWith(Parameterized.class)
@Ignore
public class AllPartitionTypesIT extends BaseHiveMetadataPropagationIT {

  private static Logger LOG = LoggerFactory.getLogger(AllPartitionTypesIT.class);

  private static List<PartitionConfig> partition(HiveType type) {
    return new PartitionConfigBuilder().addPartition("part", type, "${record:value('/col')}").build();
  }

  @Parameterized.Parameters(name = "type({0})")
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][]{
        // Supported types
        {Field.create(Field.Type.INTEGER, 1), partition(HiveType.INT), true, Types.INTEGER, 1},
        {Field.create(Field.Type.LONG, 1), partition(HiveType.BIGINT), true, Types.BIGINT, 1L},
        {Field.create(Field.Type.STRING, "value"), partition(HiveType.STRING), true, Types.VARCHAR, "value"},
        // Unsupported types
        {Field.create(Field.Type.BOOLEAN, true), partition(HiveType.BOOLEAN), false, 0, null},
        {Field.create(Field.Type.DATE, new Date()), partition(HiveType.DATE), false, 0, null},
        {Field.create(Field.Type.FLOAT, 1.2), partition(HiveType.FLOAT), false, 0, null},
        {Field.create(Field.Type.DOUBLE, 1.2), partition(HiveType.DOUBLE), false, 0, null},
        {Field.create(Field.Type.BYTE_ARRAY, new byte[] {0x00}), partition(HiveType.BINARY), false, 0, null},
        // Decimal fails with java.lang.ArrayIndexOutOfBoundsException
//      {Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(1.5)), partition(HiveType.DECIMAL), false, 0, null},
    });
  }
  private final Field field;
  private final List<PartitionConfig> partition;
  private final boolean supported;
  private final int hiveType;
  private final Object hiveValue;

  public AllPartitionTypesIT(Field field, List<PartitionConfig> partition, boolean supported, int hiveType, Object hiveValue) {
    this.field = field;
    this.partition = partition;
    this.supported = supported;
    this.hiveType = hiveType;
    this.hiveValue = hiveValue;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPartitionType() throws  Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .partitions(partition)
        .build();
    final HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("col", field);
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      processRecords(processor, hiveTarget, ImmutableList.of(record));
      if(!supported) {
        Assert.fail("Type is not supported, but yet no exception was thrown");
      }
    } catch(StageException se) {
      if(supported) {
        LOG.error("Processing exception", se);
        Assert.fail("Processing testing record unexpectedly failed: " + se.getMessage());
        throw se;
      } else {
        // We're comparing string here as the class ContainerError is not on classpath
        Assert.assertEquals("CONTAINER_0010", se.getErrorCode().getCode());
        Assert.assertTrue(se.getMessage().contains(Errors.HIVE_METADATA_09.name()));
        // No additional verification necessary
        return;
      }
    }

    assertTableExists("default.tbl");
    assertQueryResult("select * from tbl", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(
            rs,
            ImmutablePair.of("tbl.col", hiveType),
            ImmutablePair.of("tbl.part", hiveType)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(hiveValue, rs.getObject(1));
        Assert.assertEquals(hiveValue, rs.getObject(2));
        Assert.assertFalse("Table tbl contains more then one row", rs.next());
      }
    });
  }
}
