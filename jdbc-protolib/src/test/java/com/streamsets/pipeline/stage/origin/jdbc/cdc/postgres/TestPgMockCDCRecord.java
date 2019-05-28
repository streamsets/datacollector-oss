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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.postgres;

import com.streamsets.pipeline.api.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestPgMockCDCRecord {

  private Map<String, Field> cdcRecord;

  public TestPgMockCDCRecord(String xid, String lsn, String timestamp) {
    List<Field> changes = new ArrayList<Field>() {{
      add(Field.create(new TestPgMockCDCRecordChange().getCDCChange()));
    }};

    cdcRecord = new HashMap<String, Field>() {{
      put("xid", Field.create(xid));
      put("nextlsn", Field.create(lsn));
      put("timestamp", Field.create(timestamp));
      put("change", Field.create(changes));
    }};
  }

  public TestPgMockCDCRecord(String xid, String lsn, String timestamp, List<Field> changes) {
    cdcRecord = new HashMap<String, Field>() {{
      put("xid", Field.create(xid));
      put("nextlsn", Field.create(lsn));
      put("timestamp", Field.create(timestamp));
      put("change", Field.create(changes));
    }};
  }

  public Map<String, Field> getCDCRecord() {
    return cdcRecord;
  }

  public List<Field> getCDCRecordChanges() {
    return cdcRecord.get("change").getValueAsList();
  }

  public String getTimeStamp() {
    return cdcRecord.get("timestamp").getValueAsString();
  }

  public class TestPgMockCDCRecordChange {

    private Map<String, Field> cdcChange;
    private List<Field> columnNames;
    private List<Field> columnTypes;
    private List<Field> columnValues;
    private Map<String, Field> oldKeys;
    private List<Field> keyNames;
    private List<Field> keyTypes;
    private List<Field> keyValues;

    public TestPgMockCDCRecordChange() {
      columnNames = new ArrayList<Field>();
      columnTypes = new ArrayList<Field>();
      columnValues = new ArrayList<Field>();
      keyNames = new ArrayList<Field>();
      keyTypes = new ArrayList<Field>();
      keyValues = new ArrayList<Field>();

      oldKeys = new HashMap<String, Field>() {{
        put("keynames", Field.create(keyNames));
        put("keyTypes", Field.create(keyTypes));
        put("keyValues", Field.create(keyValues));
      }};

      cdcChange = new HashMap<String, Field>() {{
        put("kind", Field.create("update or insert or delete"));
        put("schema", Field.create("schemaname"));
        put("table", Field.create("tablename"));
        put("columnnames", Field.create(columnNames));
        put("columtypes", Field.create(columnTypes));
        put("columvalues", Field.create(columnValues));
        put("oldkeys", Field.create(oldKeys));
      }};
    }

    public Map<String, Field> getCDCChange() {
      return cdcChange;
    }

    public void setChangeType(String type) {
      cdcChange.put("kind", Field.create(type));
    }

    public String getChangeType() {
      return cdcChange.get("kind").getValueAsString();
    }

    public void setSchemaName(String name) {
      cdcChange.put("schema", Field.create(name));
    }

    public String getSchemaName() {
      return cdcChange.get("schema").getValueAsString();
    }

    public void setTableName(String name) {
      cdcChange.put("table", Field.create(name));
    }

    public String getTableName() {
      return cdcChange.get("table").getValueAsString();
    }

    public void addColumnNames(List<String> names) {
      for (String name : names) {
        cdcChange.get("columnnames").getValueAsList().add(Field.create(name));
      }
    }

    public void addColumnTypes(List<String> types) {
      for (String type : types) {
        cdcChange.get("columntypes").getValueAsList().add(Field.create(type));
      }
    }

    public void setOldKeys(
        List<String> keyNames,
        List<String> keyTypes,
        List<Field> keyValues) {
      oldKeys = cdcChange.get("oldkeys").getValueAsMap();

      List<Field> oldKeyKeynames = oldKeys.get("keynames").getValueAsList();
      oldKeyKeynames.clear();
      keyNames.forEach(keyname -> oldKeyKeynames.add(Field.create(keyname)));

      List<Field> oldKeyKeytypes = oldKeys.get("keytypes").getValueAsList();
      oldKeyKeytypes.clear();
      keyTypes.forEach(keytype -> oldKeyKeytypes.add(Field.create(keytype)));

      List<Field> oldKeyKeyvalues = oldKeys.get("keyvalue").getValueAsList();
      oldKeyKeyvalues.clear();
      keyValues.forEach(keyvalue -> oldKeyKeytypes.add(keyvalue.clone()));
    }


    /*
    Copping out here by only accepting "Field" as making it
    test author's responsibility to add the right base type as I can't infer from
    List<Object>
     */
    public void addColumnValues(List<Field> values) {
      for (Field value : values) {
        cdcChange.get("columnvalues").getValueAsList().add(value);
      }
    }


    public TestPgMockCDCRecordChange(Map<String, Field> newCDCChange) {
      cdcChange = newCDCChange;
    }
  }

}

/*
    This is a PostgreSQL WAL CDC record in JSON pretty print format for reference.
    This represents a multi-table, multi-row update.
    What is not shown is the actual LSN which is a separate field kept in PostgresWalRecord.class
    {
  "xid": 575,
  "nextlsn": "0/1663AA8",
  "timestamp": "2018-07-13 06:59:23.273221-07",
  "change": [
    {
      "kind": "update",
      "schema": "public",
      "table": "hashes",
      "columnnames": [
        "id",
        "value"
      ],
      "columntypes": [
        "integer",
        "character(33)"
      ],
      "columnvalues": [
        1,
        "a                                "
      ],
      "oldkeys": {
        "keynames": [
          "value"
        ],
        "keytypes": [
          "character(33)"
        ],
        "keyvalues": [
          "a                                "
        ]
      }
    },
    {
      "kind": "update",
      "schema": "public",
      "table": "hashes",
      "columnnames": [
        "id",
        "value"
      ],
      "columntypes": [
        "integer",
        "character(33)"
      ],
      "columnvalues": [
        2,
        "b                                "
      ],
      "oldkeys": {
        "keynames": [
          "value"
        ],
        "keytypes": [
          "character(33)"
        ],
        "keyvalues": [
          "b                                "
        ]
      }
    },
    {
      "kind": "update",
      "schema": "public",
      "table": "hashes",
      "columnnames": [
        "id",
        "value"
      ],
      "columntypes": [
        "integer",
        "character(33)"
      ],
      "columnvalues": [
        3,
        "c                                "
      ],
      "oldkeys": {
        "keynames": [
          "value"
        ],
        "keytypes": [
          "character(33)"
        ],
        "keyvalues": [
          "c                                "
        ]
      }
    },
    {
      "kind": "update",
      "schema": "public",
      "table": "idnames",
      "columnnames": [
        "id",
        "name"
      ],
      "columntypes": [
        "integer",
        "character varying(255)"
      ],
      "columnvalues": [
        1,
        "a"
      ],
      "oldkeys": {
        "keynames": [
          "id"
        ],
        "keytypes": [
          "integer"
        ],
        "keyvalues": [
          1
        ]
      }
    },
    {
      "kind": "update",
      "schema": "public",
      "table": "idnames",
      "columnnames": [
        "id",
        "name"
      ],
      "columntypes": [
        "integer",
        "character varying(255)"
      ],
      "columnvalues": [
        2,
        "b"
      ],
      "oldkeys": {
        "keynames": [
          "id"
        ],
        "keytypes": [
          "integer"
        ],
        "keyvalues": [
          2
        ]
      }
    },
    {
      "kind": "update",
      "schema": "public",
      "table": "idnames",
      "columnnames": [
        "id",
        "name"
      ],
      "columntypes": [
        "integer",
        "character varying(255)"
      ],
      "columnvalues": [
        3,
        "c"
      ],
      "oldkeys": {
        "keynames": [
          "id"
        ],
        "keytypes": [
          "integer"
        ],
        "keyvalues": [
          3
        ]
      }
    }
  ]
}
 */
