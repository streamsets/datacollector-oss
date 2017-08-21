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
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Try all SDC types containing NULL value to make sure that we're propagating it properly to Hive.
 */
@RunWith(Parameterized.class)
@Ignore
public class AllNullTypesIT extends BaseHiveMetadataPropagationIT {

  private static Logger LOG = LoggerFactory.getLogger(ColdStartIT.class);

  @Parameterized.Parameters(name = "type({0})")
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][]{
        {Field.create(Field.Type.BOOLEAN, null), true, Types.BOOLEAN},
        {Field.create(Field.Type.CHAR, null), true, Types.VARCHAR},
        {Field.create(Field.Type.BYTE, null), false, 0},
        {Field.create(Field.Type.SHORT, null), true, Types.INTEGER},
        {Field.create(Field.Type.INTEGER, null), true, Types.INTEGER},
        {Field.create(Field.Type.LONG, null), true, Types.BIGINT},
        {Field.create(Field.Type.FLOAT, null), true, Types.FLOAT},
        {Field.create(Field.Type.DOUBLE, null), true, Types.DOUBLE},
        {Field.create(Field.Type.DATE, null), true, Types.DATE},
        {Field.create(Field.Type.DATETIME, null), true, Types.VARCHAR},
        {Field.create(Field.Type.TIME, null), true, Types.VARCHAR},
        {Field.create(Field.Type.DECIMAL, null), true, Types.DECIMAL},
        {Field.create(Field.Type.STRING, null), true, Types.VARCHAR},
        {Field.create(Field.Type.BYTE_ARRAY, null), true, Types.BINARY},
        {Field.create(Field.Type.MAP, null), false, 0},
        {Field.create(Field.Type.LIST, null), false, 0},
        {Field.create(Field.Type.LIST_MAP, null), false, 0},
    });
  }

  private final Field field;
  private final boolean supported;
  private final int hiveType;

  public AllNullTypesIT(Field field, boolean supported, int hiveType) {
    this.field = field;
    this.supported = supported;
    this.hiveType = hiveType;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testType() throws  Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
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
        Assert.assertEquals(Errors.HIVE_19, se.getErrorCode());
        // No additional verification necessary
        return;
      }
    }

    assertTableExists("default.tbl");
    assertQueryResult("select * from tbl", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl.col", hiveType),
            ImmutablePair.of("tbl.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals(null, rs.getObject(1));
        Assert.assertFalse("Table tbl contains more then one row", rs.next());
      }
    });
  }
}
