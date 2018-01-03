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

package com.streamsets.pipeline.lib.util;

import com.streamsets.datacollector.el.ELEvaluator;
import com.streamsets.datacollector.el.ELVariables;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.FieldEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.testing.fieldbuilder.MapFieldBuilder;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class TestFieldPathExpressionUtil {

  @Test
  public void testFieldExpressions() throws ELEvalException {

    ELEval eval = new ELEvaluator("testFieldExpressions", TimeNowEL.class, RecordEL.class, FieldEL.class);
    ELVars variables = new ELVariables();

    Record record1 = new RecordImpl("testFieldExpressions", "record1", null, null);
    record1.set(MapFieldBuilder.builder().add("int1", 100).add("int2", 50).add("int3", 150).build());

    assertExpressionMatches(
        "/*[${f:value() < 101}]",
        eval,
        variables,
        record1,
        "/int1",
        "/int2"
    );

    Record record2 = new RecordImpl("testFieldExpressions", "record2", null, null);
    record2.set(MapFieldBuilder.builder().add("int1", 20).add("int2", 175).add("int3", 99).add("int4", 50).build());

    assertExpressionMatches(
        "/*[${f:value() < record:value('/int3') or str:contains(f:path(), '4')}]",
        eval,
        variables,
        record2,
        "/int1",
        "/int4"
    );
  }

  @Test
  public void testMultipleNestedExpressions() throws ELEvalException {

    ELEval eval = new ELEvaluator("testMultipleNestedExpressions", TimeNowEL.class, RecordEL.class, FieldEL.class);
    ELVars variables = new ELVariables();

    Record record1 = new RecordImpl("testMultipleNestedExpressions", "record1", null, null);

    final MapFieldBuilder builder = MapFieldBuilder.builder();
    builder.startMap("first")
        .startMap("first_1").add("first_1_a", "a").add("first_1_b", 14).end()
        .startMap("first_2").add("first_2_c", 17).add("first_2_d", "d").end()
        .end("source", "foo")
        .startMap("second")
        .startMap("second_1").add("second_1_a", "a").add("second_1_b", "b").end()
        .startMap("second_2").add("second_2_c", "c").add("second_2_d", "d").end()
        .end("source", "bar", "otherAttr", "nothing")
        .startMap("third")
        .startMap("third_1")
        .startMap("third_1_1").add("third_1_1_a", "a").add("third_1_1_b", "b").end()
        .startMap("third_1_2").add("third_1_2_c", "c").add("third_1_2_d", "d").end()
        .end()
        .startMap("third_2")
        .startMap("third_2_1").add("third_2_1_1", "third_2_1_1_val").add("third_2_1_2", 8).end()
        .startMap("third_2_2").add("third_2_2_1", 3).add("third_2_2_2", 5).end()
        .end()
        .startMap("third_3").add("third_3_1", "bar").add("third_3_2", 78).end()
        .end("source", "foo");

    record1.set(builder.build());

    assertExpressionMatches(
        "/*[${f:attribute('source') == 'foo'}]",
        eval,
        variables,
        record1,
        "/first",
        "/third"
    );

    assertExpressionMatches(
        "/*[${f:attribute('source') == 'foo'}]/*/*[${f:type() == 'STRING'}]",
        eval,
        variables,
        record1,
        "/first/first_1/first_1_a",
        "/first/first_2/first_2_d",
        "/third/third_3/third_3_1"
    );
  }

  private static void assertExpressionMatches(
      String expression,
      ELEval eval,
      ELVars vars,
      Record record,
      String... expectedMatches
  ) throws ELEvalException {

    List<String> matchingPaths = FieldPathExpressionUtil.evaluateMatchingFieldPaths(
        expression,
        eval,
        vars,
        record
    );

    assertThat(matchingPaths, hasSize(expectedMatches.length));
    for (int i = 0; i < expectedMatches.length; i++) {
      assertThat(matchingPaths.get(i), equalTo(expectedMatches[i]));
    }
  }
}
