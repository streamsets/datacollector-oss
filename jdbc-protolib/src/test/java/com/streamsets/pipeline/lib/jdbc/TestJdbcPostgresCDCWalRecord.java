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
package com.streamsets.pipeline.lib.jdbc;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.DecoderValues;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres.PostgresWalRecord;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.replication.LogSequenceNumber;

public class TestJdbcPostgresCDCWalRecord {


  private static final String updateTwoTablesManyRows = "{\"xid\":598,\"nextlsn\":\"0/16751E8\",\"timestamp\":\"2018-07-13 13:24:44.152109-07\",\"change\":[{\"kind\":\"update\",\"schema\":\"public\",\"table\":\"hashes\",\"columnnames\":[\"id\",\"value\"],\"columntypes\":[\"integer\",\"character(33)\"],\"columnvalues\":[1,\"a                                \"],\"oldkeys\":{\"keynames\":[\"value\"],\"keytypes\":[\"character(33)\"],\"keyvalues\":[\"a                                \"]}},{\"kind\":\"update\",\"schema\":\"public\",\"table\":\"hashes\",\"columnnames\":[\"id\",\"value\"],\"columntypes\":[\"integer\",\"character(33)\"],\"columnvalues\":[2,\"b                                \"],\"oldkeys\":{\"keynames\":[\"value\"],\"keytypes\":[\"character(33)\"],\"keyvalues\":[\"b                                \"]}},{\"kind\":\"update\",\"schema\":\"public\",\"table\":\"hashes\",\"columnnames\":[\"id\",\"value\"],\"columntypes\":[\"integer\",\"character(33)\"],\"columnvalues\":[3,\"c                                \"],\"oldkeys\":{\"keynames\":[\"value\"],\"keytypes\":[\"character(33)\"],\"keyvalues\":[\"c                                \"]}},{\"kind\":\"update\",\"schema\":\"public\",\"table\":\"idnames\",\"columnnames\":[\"id\",\"name\"],\"columntypes\":[\"integer\",\"character varying(255)\"],\"columnvalues\":[1,\"a\"],\"oldkeys\":{\"keynames\":[\"id\"],\"keytypes\":[\"integer\"],\"keyvalues\":[1]}},{\"kind\":\"update\",\"schema\":\"public\",\"table\":\"idnames\",\"columnnames\":[\"id\",\"name\"],\"columntypes\":[\"integer\",\"character varying(255)\"],\"columnvalues\":[2,\"b\"],\"oldkeys\":{\"keynames\":[\"id\"],\"keytypes\":[\"integer\"],\"keyvalues\":[2]}},{\"kind\":\"update\",\"schema\":\"public\",\"table\":\"idnames\",\"columnnames\":[\"id\",\"name\"],\"columntypes\":[\"integer\",\"character varying(255)\"],\"columnvalues\":[3,\"c\"],\"oldkeys\":{\"keynames\":[\"id\"],\"keytypes\":[\"integer\"],\"keyvalues\":[3]}}]}";


  @Test
  public void testWalRecordFromString() {
    String jsonRecord = updateTwoTablesManyRows;

    ByteBuffer bb = ByteBuffer.wrap(jsonRecord.getBytes());

    PostgresWalRecord walRecord = new PostgresWalRecord(
        bb,
        LogSequenceNumber.valueOf("0/0"),
        DecoderValues.WAL2JSON);

    Assert.assertEquals(jsonRecord + " LSN: 0/0", walRecord.toString());
    Assert.assertEquals(walRecord.getXid(),"598");
    Assert.assertEquals(walRecord.getTimestamp(),"2018-07-13 13:24:44.152109-07");

    List<Field> changes = walRecord.getChanges();
    Assert.assertEquals(changes.size(), 6);

    List<String> validTables = ImmutableList.of("idnames", "hashes");
    changes.forEach(change -> {
      Assert.assertEquals("update",
          PostgresWalRecord.getTypeFromChangeMap(change.getValueAsMap()));
      Assert.assertEquals("public",
          PostgresWalRecord.getSchemaFromChangeMap(change.getValueAsMap()));
      Assert.assertTrue(validTables.contains(
          PostgresWalRecord.getTableFromChangeMap(change.getValueAsMap())));
    });

  }

  @Test
  public void testWalRecordBadDecoder() {
    String jsonRecord = updateTwoTablesManyRows;

    ByteBuffer bb = ByteBuffer.wrap(jsonRecord.getBytes());

    PostgresWalRecord walRecord = new PostgresWalRecord(
        bb,
        LogSequenceNumber.valueOf("0/0"),
        null);

    Assert.assertNull(walRecord.getField());
  }

  @Test
  public void testWalRecordWal2JsonDecoderGood() {
    String jsonRecord = updateTwoTablesManyRows;

    ByteBuffer bb = ByteBuffer.wrap(jsonRecord.getBytes());

    PostgresWalRecord walRecord = new PostgresWalRecord(
        bb,
        LogSequenceNumber.valueOf("0/0"),
        DecoderValues.WAL2JSON);

    Assert.assertNotNull(walRecord.getField());
  }

  @Test
  public void testWalRecordWal2JsonDecoderBad() {
    String jsonRecord = "I am just a string";

    ByteBuffer bb = ByteBuffer.wrap(jsonRecord.getBytes());

    PostgresWalRecord walRecord = new PostgresWalRecord(
        bb,
        LogSequenceNumber.valueOf("0/0"),
        DecoderValues.WAL2JSON);

    Assert.assertNull(walRecord.getField());
  }

}
