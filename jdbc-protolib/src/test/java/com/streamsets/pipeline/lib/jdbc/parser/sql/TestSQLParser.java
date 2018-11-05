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
package com.streamsets.pipeline.lib.jdbc.parser.sql;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.lib.jdbc.OracleCDCOperationCode;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.parboiled.Parboiled;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@RunWith(Parameterized.class)
public class TestSQLParser {

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws Exception {

    return Arrays.asList(new Object[][]
        {
            {
                "insert into \"SYS\".\"MANYCOLS\"(\"ID\",\"NAME\",\"HIREDATE\",\"SALARY\",\"LASTLOGIN\") " +
                    "values ('1','sdc', TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS')," +
                    "'1332.332',TO_TIMESTAMP('2016-11-21 11:34:09.982753'))"
                ,
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("NAME", "sdc");
                    put("HIREDATE", "TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS')");
                    put("SALARY", "1332.332");
                    put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");
                  }
                }

            },
            {
                "insert into \"SYS\".\"MANYCOLS\"(\"ID\",\"NAME\",\"HIREDATE\",\"SALARY\",\"LASTLOGIN\") " +
                    "values ('10','stream',TO_DATE('19-11-2016 11:35:16', 'DD-MM-YYYY HH24:MI:SS'),'10000.1',NULL)",
                new HashMap<String, String>() {
                  {
                    put("ID", "10");
                    put("NAME", "stream");
                    put("HIREDATE", "TO_DATE('19-11-2016 11:35:16', 'DD-MM-YYYY HH24:MI:SS')");
                    put("SALARY", "10000.1");
                    put("LASTLOGIN", null);
                  }
                }
            },
            {
                " update \"SYS\".\"MANYCOLS\" set \"SALARY\" = '1998.483' " +
                    "where \"ID\" = '1' and \"NAME\" IS NULL and" +
                    " \"HIREDATE\" = TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS') and " +
                    "\"SALARY\" = '1332.322' and \"LASTLOGIN\" = TO_TIMESTAMP('2016-11-21 11:34:09.982753')" +
                    " and rowid = 'Addajkdajkd'",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("SALARY", "1998.483");
                    put("NAME", null);
                    put("HIREDATE", "TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS')");
                    put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");
                    put("ROWID", "Addajkdajkd");
                  }
                }
            },
            {
                " update \"SYS\".\"MANYCOLS\" A set A.\"SALARY\" = '1998.483' " +
                    "where A.\"ID\" = '1' and A.\"NAME\" IS NULL and" +
                    " A.\"HIREDATE\" = TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS') and " +
                    "A.\"SALARY\" = '1332.322' and A.\"LASTLOGIN\" = TO_TIMESTAMP('2016-11-21 11:34:09.982753')" +
                    " and A.rowid = 'Addajkdajkd'",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("SALARY", "1998.483");
                    put("NAME", null);
                    put("HIREDATE", "TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS')");
                    put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");
                    put("ROWID", "Addajkdajkd");
                  }
                }
            },
            {" update \"SYS\".\"MANYCOLS\" set \"SALARY=\" = '1998.483' " +
                "where \"ID\" = '1' and \"NAME\" = '=sdc' and" +
                " \"HIREDATE\" = TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS') and " +
                "\"SALARY=\" = '1332.322' and \"LASTLOGIN\" = TO_TIMESTAMP('2016-11-21 11:34:09.982753')" +
                " and ROWID = 'rowowowow'",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("SALARY=", "1998.483");
                    put("NAME", "=sdc");
                    put("HIREDATE", "TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS')");
                    put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");
                    put("ROWID", "rowowowow");

                  }
                }
            },
            {" update \"SYS\".\"MANYCOLS\" set \"SALARY=\" = '1998.483', \"NAME\" = NULL " +
                "where \"ID\" = '1' and \"NAME\" = '=sdc' and" +
                " \"HIREDATE\" = TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS') and " +
                "\"SALARY=\" = '1332.322' and \"LASTLOGIN\" = TO_TIMESTAMP('2016-11-21 11:34:09.982753')" +
                " and ROWID = 'poiuyttuoo'",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("SALARY=", "1998.483");
                    put("NAME", null);
                    put("HIREDATE", "TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS')");
                    put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");
                    put("ROWID", "poiuyttuoo");

                  }
                }
            },
            {" update \"SYS\".\"MANYCOLS\" set \"SALARY=\" = NULL, \"NAME\" = 'New Name' " +
                "where \"ID\" = '1' and \"NAME\" = '=sdc' and" +
                " \"HIREDATE\" = TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS') and " +
                "\"SALARY=\" = '1332.322' and \"LASTLOGIN\" = TO_TIMESTAMP('2016-11-21 11:34:09.982753')" +
                " and rowid = 'bobcat'",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("SALARY=", null);
                    put("NAME", "New Name");
                    put("HIREDATE", "TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS')");
                    put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");
                    put("ROWID", "bobcat");

                  }
                }
            },
            {
                "delete from \"SYS\".\"MANYCOLS\" where \"ID\" = '10' and \"NAME\" = 'stream' and " +
                    "\"HIREDATE\" = TO_DATE('19-11-2016 11:35:16', 'DD-MM-YYYY HH24:MI:SS') and " +
                    "\"SALARY\" = '10000.1' and \"LASTLOGIN\" IS NULL and ROWID = 'AASDDxs'\n",
                new HashMap<String, String>() {
                  {
                    put("ID", "10");
                    put("NAME", "stream");
                    put("HIREDATE", "TO_DATE('19-11-2016 11:35:16', 'DD-MM-YYYY HH24:MI:SS')");
                    put("SALARY", "10000.1");
                    put("LASTLOGIN", null);
                    put("ROWID", "AASDDxs");
                  }
                }
            },
            {
                "delete from \"SYS\".\"MANYCOLS\" A where A.\"ID\" = '10' and A.\"NAME\" = 'stream' and " +
                    "A.\"HIREDATE\" = TO_DATE('19-11-2016 11:35:16', 'DD-MM-YYYY HH24:MI:SS') and " +
                    "A.\"SALARY\" = '10000.1' and A.\"LASTLOGIN\" IS NULL and A.ROWID = 'AASDDxs'\n",
                new HashMap<String, String>() {
                  {
                    put("ID", "10");
                    put("NAME", "stream");
                    put("HIREDATE", "TO_DATE('19-11-2016 11:35:16', 'DD-MM-YYYY HH24:MI:SS')");
                    put("SALARY", "10000.1");
                    put("LASTLOGIN", null);
                    put("ROWID", "AASDDxs");
                  }
                }
            },
            {
                "insert into \"SYS\".\"WIN\" (\"ID\", \"DESC\", \"STATUS\", \"TOTAL\", \"DATE1\", \"DATE2\") " +
                    "values ('1', 'This is a \r\n test \r\n that tests windows \r\n line endings\r', 'COMPLETE', '45', " +
                    "TO_DATE('21-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS'), " +
                    "TO_DATE('10-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS'))",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("DESC", "This is a \r\n test \r\n that tests windows \r\n line endings\r");
                    put("STATUS", "COMPLETE");
                    put("TOTAL", "45");
                    put("DATE1", "TO_DATE('21-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS')");
                    put("DATE2", "TO_DATE('10-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS')");
                  }
                }
            },
            {
                "insert into \"SYS\".\"UNIX\" (\"ID\", \"DESC\", \"STATUS\", \"TOTAL\", \"DATE1\", \"DATE2\") " +
                    "values ('1', 'This is a \n test \n that tests windows \n line endings\n', 'COMPLETE', '45', " +
                    "TO_DATE('21-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS'), " +
                    "TO_DATE('10-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS'))",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("DESC", "This is a \n test \n that tests windows \n line endings\n");
                    put("STATUS", "COMPLETE");
                    put("TOTAL", "45");
                    put("DATE1", "TO_DATE('21-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS')");
                    put("DATE2", "TO_DATE('10-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS')");
                  }
                }
            },
            {
                "insert into \"SYS\".\"PROPER_WIN\" (\"ID\", \"DESC\", \"STATUS\", \"TOTAL\", \"DATE1\", \"DATE2\") " +
                    "values ('1', 'This is a \r\n test \r\n that tests windows \r\n line endings\r\n', 'COMPLETE', '45', " +
                    "TO_DATE('21-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS'), " +
                    "TO_DATE('10-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS'))",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("DESC", "This is a \r\n test \r\n that tests windows \r\n line endings\r\n");
                    put("STATUS", "COMPLETE");
                    put("TOTAL", "45");
                    put("DATE1", "TO_DATE('21-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS')");
                    put("DATE2", "TO_DATE('10-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS')");
                  }
                }
            },
            {
                "insert into \"SYS\".\"MIX\" (\"ID\", \"DESC\", \"STATUS\", \"TOTAL\", \"DATE1\", \"DATE2\") " +
                    "values ('1', 'This is a \r test \n that tests windows \r\n line endings\r', 'COMPLETE', '45', " +
                    "TO_DATE('21-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS'), " +
                    "TO_DATE('10-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS'))",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("DESC", "This is a \r test \n that tests windows \r\n line endings\r");
                    put("STATUS", "COMPLETE");
                    put("TOTAL", "45");
                    put("DATE1", "TO_DATE('21-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS')");
                    put("DATE2", "TO_DATE('10-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS')");
                  }
                }
            },
            {
                "insert into \"SYS\".\"MIX\" (\"ID\", \"DESC\", \"STATUS\", \"TOTAL\", \"DATE1\", \"DATE2\") " +
                    "values ('1', 'This is a test     that tests no line     endings', 'COMPLETE', '45', " +
                    "TO_DATE('21-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS'), " +
                    "TO_DATE('10-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS'))",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("DESC", "This is a test     that tests no line     endings");
                    put("STATUS", "COMPLETE");
                    put("TOTAL", "45");
                    put("DATE1", "TO_DATE('21-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS')");
                    put("DATE2", "TO_DATE('10-12-2017 14:13:04', 'DD-MM-YYYY HH24:MI:SS')");
                  }
                }
            },
            {" update \"SYS\".\"MANYCOLS\" set \"SALARY=\" = NULL, \"NAME\" = 'New Name' " +
                "where \"ID\" = '1' and \"NAME\" = '=sdc' and" +
                " \"HIREDATE\" = TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS') and " +
                "\"SALARY=\" = '1332.322' and \"LASTLOGIN\" = TO_TIMESTAMP('2016-11-21 11:34:09.982753') and ROWID = " +
                "'AAAAxhdjhjsdhaks'",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("SALARY=", null);
                    put("NAME", "New Name");
                    put("HIREDATE", "TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS')");
                    put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");
                    put("ROWID", "AAAAxhdjhjsdhaks");

                  }
                }
            }
        }
    );
  }

  private final String sql;
  private final Map<String, String> expected;

  public TestSQLParser(String sql, Map<String, String> expected) {
    this.sql = sql;
    this.expected = expected;
  }


  @Test
  public void testSQL() throws Exception {
    SQLParser parser = Parboiled.createParser(SQLParser.class);
    Map<String, String> colVals;
    int code;
    if (sql.startsWith("insert")) {
      code = OracleCDCOperationCode.INSERT_CODE;
    } else if (sql.startsWith("delete")) {
      code = OracleCDCOperationCode.DELETE_CODE;
    } else {
      code = OracleCDCOperationCode.UPDATE_CODE;
    }
    colVals = SQLParserUtils.process(parser, sql, code, false, false, null);

    Assert.assertEquals(expected, colVals);
  }

  @Test
  public void testSQLWithNulls() throws Exception {
    String sqlInternal = " update \"SYS\".\"MANYCOLS\" set \"SALARY=\" = NULL, \"NAME\" = 'New Name' " +
        "where \"ID\" = '1' and \"NAME\" = '=sdc' and" +
        " \"HIREDATE\" = TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS') and " +
        "\"SALARY=\" = '1332.322' and \"LASTLOGIN\" = TO_TIMESTAMP('2016-11-21 11:34:09.982753') and ROWID = " +
        "'AAAAxhdjhjsdhaks'";
    doTestSQLWithNulls(sqlInternal);
  }

  private void doTestSQLWithNulls(String sqlInternal) throws UnparseableSQLException {
    SQLParser parser = Parboiled.createParser(SQLParser.class);
    Map<String, String> colVals;
    int code = OracleCDCOperationCode.UPDATE_CODE;
    Set<String> expectedFields = new HashSet<>(
        Lists.newArrayList("ID", "NAME", "HIREDATE", "SALARY=", "LASTLOGIN", "LASTDATE"));
    colVals = SQLParserUtils.process(parser, sqlInternal, code, true, false, expectedFields);
    Map<String, String> exp = new HashMap<String, String>() {
      {
        put("ID", "1");
        put("SALARY=", null);
        put("NAME", "New Name");
        put("HIREDATE", "TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS')");
        put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");
        put("ROWID", "AAAAxhdjhjsdhaks");
        put("LASTDATE", null);
      }
    };
    Assert.assertEquals(exp, colVals);
  }

  @Test
  public void testSQLWithNullsTableAlias() throws Exception {
    String sqlInternal = " update \"SYS\".\"MANYCOLS\" a set a.\"SALARY=\" = NULL, a.\"NAME\" = 'New Name' " +
        "where a.\"ID\" = '1' and a.\"NAME\" = '=sdc' and" +
        " a.\"HIREDATE\" = TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS') and " +
        "a.\"SALARY=\" = '1332.322' and a.\"LASTLOGIN\" = TO_TIMESTAMP('2016-11-21 11:34:09.982753') and a.ROWID = " +
        "'AAAAxhdjhjsdhaks'";
    doTestSQLWithNulls(sqlInternal);
  }

  @Test (expected = UnparseableSQLException.class)
  public void testInvalidSql() throws Exception {
    String sqlInternal = " update \"SYS\".\"MANYCOLS\" set why are we testing this";
    SQLParser parser = Parboiled.createParser(SQLParser.class);
    Map<String, String> colVals;
    int code = OracleCDCOperationCode.UPDATE_CODE;
    Set<String> expectedFields = new HashSet<>(
        Lists.newArrayList("ID", "NAME", "HIREDATE", "SALARY=", "LASTLOGIN", "LASTDATE"));
    colVals = SQLParserUtils.process(parser, sqlInternal, code, true, false, expectedFields);
    Map<String, String> exp = new HashMap<String, String>() {
      {
        put("ID", "1");
        put("SALARY=", null);
        put("NAME", "New Name");
        put("HIREDATE", "TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS')");
        put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");
        put("ROWID", "AAAAxhdjhjsdhaks");
        put("LASTDATE", null);
      }
    };
    Assert.assertEquals(exp, colVals);
  }

  @Test
  public void testFormat() {

    Assert.assertEquals("Mithrandir", SQLParserUtils.format("Mithrandir"));
    Assert.assertEquals("Greyhame", SQLParserUtils.format("\'Greyhame\'"));
    Assert.assertEquals("Stormcrow", SQLParserUtils.format("\"Stormcrow\""));
    Assert.assertEquals("Lathspell", SQLParserUtils.format("\"\'Lathspell\'\""));
  }

}