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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import plsql.plsqlLexer;
import plsql.plsqlParser;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestSQLListener {

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
                    put("HIREDATE", "TO_DATE('21-11-2016 11:34:09','DD-MM-YYYY HH24:MI:SS')");
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
                    put("HIREDATE", "TO_DATE('19-11-2016 11:35:16','DD-MM-YYYY HH24:MI:SS')");
                    put("SALARY", "10000.1");
                    put("LASTLOGIN", "NULL"); //inserts just return string "NULL" which we handle in the origin itself.
                  }
                }
            },
            {
                " update \"SYS\".\"MANYCOLS\" set \"SALARY\" = '1998.483' " +
                    "where \"ID\" = '1' and \"NAME\" IS NULL and" +
                    " \"HIREDATE\" = TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS') and " +
                    "\"SALARY\" = '1332.322' and \"LASTLOGIN\" = TO_TIMESTAMP('2016-11-21 11:34:09.982753')",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("SALARY", "1998.483");
                    put("NAME", null);
                    put("HIREDATE", "TO_DATE('21-11-2016 11:34:09','DD-MM-YYYY HH24:MI:SS')");
                    put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");
                  }
                }
            },
            {" update \"SYS\".\"MANYCOLS\" set \"SALARY=\" = '1998.483' " +
                "where \"ID\" = '1' and \"NAME\" = '=sdc' and" +
                " \"HIREDATE\" = TO_DATE('21-11-2016 11:34:09', 'DD-MM-YYYY HH24:MI:SS') and " +
                "\"SALARY=\" = '1332.322' and \"LASTLOGIN\" = TO_TIMESTAMP('2016-11-21 11:34:09.982753')",
                new HashMap<String, String>() {
                  {
                    put("ID", "1");
                    put("SALARY=", "1998.483");
                    put("NAME", "=sdc");
                    put("HIREDATE", "TO_DATE('21-11-2016 11:34:09','DD-MM-YYYY HH24:MI:SS')");
                    put("LASTLOGIN", "TO_TIMESTAMP('2016-11-21 11:34:09.982753')");

                  }
                }
            },
            {
              "delete from \"SYS\".\"MANYCOLS\" where \"ID\" = '10' and \"NAME\" = 'stream' and " +
                  "\"HIREDATE\" = TO_DATE('19-11-2016 11:35:16', 'DD-MM-YYYY HH24:MI:SS') and " +
                  "\"SALARY\" = '10000.1' and \"LASTLOGIN\" IS NULL\n",
                new HashMap<String, String>() {
                  {
                    put("ID", "10");
                    put("NAME", "stream");
                    put("HIREDATE", "TO_DATE('19-11-2016 11:35:16','DD-MM-YYYY HH24:MI:SS')");
                    put("SALARY", "10000.1");
                    put("LASTLOGIN", null);
                  }
                }
            }
        }
    );
  }

  private final String sql;
  private final Map<String, String> expected;

  public TestSQLListener(String sql, Map<String, String> expected) {
    this.sql = sql;
    this.expected = expected;
  }

  @Test
  public void testSQL() {
    plsqlLexer l = new plsqlLexer(new ANTLRInputStream(sql));
    CommonTokenStream str = new CommonTokenStream(l);
    plsqlParser parser = new plsqlParser(str);
    ParserRuleContext c;
    if (sql.startsWith("insert")) {
      c = parser.insert_statement();
    } else if (sql.startsWith("delete")) {
      c = parser.delete_statement();
    } else {
      c = parser.update_statement();
    }
    SQLListener sqlListener = new SQLListener();
    sqlListener.allowNulls();
    sqlListener.setColumns(new HashSet<>(Lists.newArrayList("ID", "NAME", "HIREDATE", "SALARY", "LASTLOGIN")));
    ParseTreeWalker parseTreeWalker = new ParseTreeWalker();
    // Walk it and attach our sqlListener
    parseTreeWalker.walk(sqlListener, c);
    Assert.assertEquals(expected, sqlListener.getColumns());
  }

  @Test
  public void testFormat() {
    SQLListener listener = new SQLListener();

    Assert.assertEquals("Mithrandir", listener.format("Mithrandir"));
    Assert.assertEquals("Greyhame", listener.format("\'Greyhame\'"));
    Assert.assertEquals("Stormcrow", listener.format("\"Stormcrow\""));
    Assert.assertEquals("Lathspell", listener.format("\"\'Lathspell\'\""));
  }
}
