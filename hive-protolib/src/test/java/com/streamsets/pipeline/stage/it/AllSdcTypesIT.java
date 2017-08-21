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
import com.streamsets.pipeline.stage.processor.hive.DecimalDefaultsConfig;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Run all various data types that are available in SDC to validate that they behave in expected way.
 */
@RunWith(Parameterized.class)
@Ignore
public class AllSdcTypesIT extends BaseHiveMetadataPropagationIT {

  private static Logger LOG = LoggerFactory.getLogger(ColdStartIT.class);

  private static final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
  private static Date date = new Date();

  @Parameterized.Parameters(name = "type({0})")
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][]{
        {Field.create(Field.Type.BOOLEAN, true), true, Types.BOOLEAN, true},
        {Field.create(Field.Type.CHAR, 'A'), true, Types.VARCHAR, "A"},
        {Field.create(Field.Type.BYTE, (byte)0x00), false, 0, null},
        {Field.create(Field.Type.SHORT, 10), true, Types.INTEGER, 10},
        {Field.create(Field.Type.INTEGER, 10), true, Types.INTEGER, 10},
        {Field.create(Field.Type.LONG, 10), true, Types.BIGINT, 10L},
        {Field.create(Field.Type.FLOAT, 1.5), true, Types.FLOAT, 1.5},
        {Field.create(Field.Type.DOUBLE, 1.5), true, Types.DOUBLE, 1.5},
        {Field.create(Field.Type.DATE, new Date(116, 5, 13)), true, Types.DATE, new Date(116, 5, 13)},
        {Field.create(Field.Type.DATETIME, date), true, Types.VARCHAR, datetimeFormat.format(date)},
        {Field.create(Field.Type.TIME, date), true, Types.VARCHAR, timeFormat.format(date)},
        {Field.create(Field.Type.DECIMAL, BigDecimal.valueOf(1.5)),
            true,
            Types.DECIMAL,
            new BigDecimal(BigInteger.valueOf(15), 1, new MathContext(2, RoundingMode.FLOOR))
        },
        {Field.create(Field.Type.STRING, "StreamSets"), true, Types.VARCHAR, "StreamSets"},
        {Field.create(Field.Type.BYTE_ARRAY, new byte[] {(byte)0x00}), true, Types.BINARY, new byte [] {(byte)0x00}},
        {Field.create(Field.Type.MAP, Collections.emptyMap()), false, 0, null},
        {Field.create(Field.Type.LIST, Collections.emptyList()), false, 0, null},
        {Field.create(Field.Type.LIST_MAP, new LinkedHashMap<>()), false, 0, null},
    });
  }

  private final Field field;
  private final boolean supported;
  private final int hiveType;
  private final Object hiveValue;

  public AllSdcTypesIT(Field field, boolean supported, int hiveType, Object hiveValue) {
    this.field = field;
    this.supported = supported;
    this.hiveType = hiveType;
    this.hiveValue = hiveValue;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testType() throws  Exception {
    DecimalDefaultsConfig decimalDefaultsConfig = new DecimalDefaultsConfig();
    decimalDefaultsConfig.scaleExpression =
        "${record:attributeOrDefault(str:concat(str:concat('jdbc.', str:toUpper(field:field())), '.scale'), 2)}";
    decimalDefaultsConfig.precisionExpression =
        "${record:attributeOrDefault(str:concat(str:concat('jdbc.', str:toUpper(field:field())), '.precision'), 2)}";
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder().decimalConfig(decimalDefaultsConfig)
        .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .build();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("col", field);
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));
    record.getHeader().setAttribute("jdbc.COL.scale", "1");
    //So scale - 1 , precision -1 (at last as scale is set to 1, precision is not set ( default is 2))
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
        if(hiveValue.getClass().isArray()) { // Only supported array is a byte array
          Assert.assertArrayEquals((byte [])hiveValue, (byte [])rs.getObject(1));
        } else {
          Assert.assertEquals(hiveValue, rs.getObject(1));
        }
        Assert.assertFalse("Table tbl contains more then one row", rs.next());
      }
    });
  }
}
