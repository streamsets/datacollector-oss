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
package com.streamsets.datacollector.record;

import com.streamsets.pipeline.api.impl.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class TestPathElement {

  @Test
  public void testMapElement() {
    PathElement hello = PathElement.createMapElement("hello");
    Assert.assertEquals("hello", hello.getName());
    Assert.assertEquals(PathElement.Type.MAP, hello.getType());
    Assert.assertEquals(0, hello.getIndex());
  }

  @Test
  public void testArrayElement() {
    PathElement hello = PathElement.createArrayElement(5);
    Assert.assertNull(hello.getName());
    Assert.assertEquals(PathElement.Type.LIST, hello.getType());
    Assert.assertEquals(5, hello.getIndex());
  }

  @Test
  public void testParseMap() {
    List<PathElement> parse = PathElement.parse("/Asia/China/Bejing", true);
    Assert.assertEquals(4, parse.size());
    Assert.assertEquals(PathElement.ROOT, parse.get(0));
    Assert.assertEquals("Asia", parse.get(1).getName());
    Assert.assertEquals(PathElement.Type.MAP,  parse.get(1).getType());
    Assert.assertEquals("China", parse.get(2).getName());
    Assert.assertEquals(PathElement.Type.MAP,  parse.get(2).getType());
    Assert.assertEquals("Bejing", parse.get(3).getName());
    Assert.assertEquals(PathElement.Type.MAP,  parse.get(3).getType());
  }

  @Test
  public void testParseMapWithWildCard() {
    List<PathElement> parse = PathElement.parse("/Asia/*/Bejing", true);
    Assert.assertEquals(4, parse.size());
    Assert.assertEquals(PathElement.ROOT, parse.get(0));
    Assert.assertEquals("Asia", parse.get(1).getName());
    Assert.assertEquals(PathElement.Type.MAP,  parse.get(1).getType());
    Assert.assertEquals("*", parse.get(2).getName());
    Assert.assertEquals(PathElement.Type.MAP,  parse.get(2).getType());
    Assert.assertEquals("Bejing", parse.get(3).getName());
    Assert.assertEquals(PathElement.Type.MAP,  parse.get(3).getType());
  }

  @Test
  public void testParseList() {
    List<PathElement> parse = PathElement.parse("[5]/China/Bejing", true);
    Assert.assertEquals(4, parse.size());
    Assert.assertEquals(PathElement.ROOT, parse.get(0));
    Assert.assertEquals(null, parse.get(1).getName());
    Assert.assertEquals(5, parse.get(1).getIndex());
    Assert.assertEquals(PathElement.Type.LIST,  parse.get(1).getType());
    Assert.assertEquals("China", parse.get(2).getName());
    Assert.assertEquals(PathElement.Type.MAP,  parse.get(2).getType());
    Assert.assertEquals("Bejing", parse.get(3).getName());
    Assert.assertEquals(PathElement.Type.MAP,  parse.get(3).getType());
  }

  @Test
  public void testParseListWildCard() {
    List<PathElement> parse = PathElement.parse("[*]/China/Bejing", true);
    Assert.assertEquals(4, parse.size());
    Assert.assertEquals(PathElement.ROOT, parse.get(0));
    Assert.assertEquals(null, parse.get(1).getName());
    Assert.assertEquals(PathElement.WILDCARD_INDEX_ANY_LENGTH, parse.get(1).getIndex());
    Assert.assertEquals(PathElement.Type.LIST,  parse.get(1).getType());
    Assert.assertEquals("China", parse.get(2).getName());
    Assert.assertEquals(PathElement.Type.MAP,  parse.get(2).getType());
    Assert.assertEquals("Bejing", parse.get(3).getName());
    Assert.assertEquals(PathElement.Type.MAP,  parse.get(3).getType());
  }

  @Test
  public void testTrailingSlash() {
    try {
      PathElement.parse("/a/b/c/d/", true);
      Assert.fail("Should fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(Utils.format("Message: {}", e.getMessage()), e.getMessage().contains(PathElement.REASON_EMPTY_FIELD_NAME));
    }
  }

  @Test
  public void testInvalidStart() {
    try {
      PathElement.parse("a/b/c/d", true);
      Assert.fail("Should fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(Utils.format("Message: {}", e.getMessage()), e.getMessage().contains(PathElement.REASON_INVALID_START));
    }
  }

  @Test
  public void testInvalidExpressions() {
    for (String expr : new String[] {"[0*8]", "[Nan]", "[{invalid_EL_1}]", "[${invalid_EL_2)]"}) {
      try {
        PathElement.parse(expr, true);
        Assert.fail("Should fail.");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(Utils.format("Message: {}", e.getMessage()), e.getMessage().contains(PathElement.REASON_NOT_VALID_EXPR));
      }
    }
  }

  @Test
  public void testOpenQuotes() {
    try {
      PathElement.parse("/'not-closed", true);
      Assert.fail("Should fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(Utils.format("Message: {}", e.getMessage()), e.getMessage().contains(PathElement.REASON_QUOTES));
    }
  }

  @Test
  public void testFieldExpression() {
    final String elExpression = "${f:type() == 'STRING' and str:matches(f:value(), '[0-9]*')}";
    final String expression = String.format("/sensor*[%s]", elExpression);

    // first, parse using the overload that simulates previous behavior (omitting field expressions)
    List<PathElement> pathElements = PathElement.parse(
        expression,
        false
    );
    assertThat(pathElements, hasSize(2));
    assertThat(pathElements.get(0).getType(), equalTo(PathElement.Type.ROOT));
    assertThat(pathElements.get(1).getType(), equalTo(PathElement.Type.MAP));

    // parse again, but this time including field expressions (which should become the "name")
    List<PathElement> pathElementsWithExpression = PathElement.parse(
        expression,
        false,
        true
    );
    assertThat(pathElementsWithExpression, hasSize(3));
    assertThat(pathElementsWithExpression.get(0).getType(), equalTo(PathElement.Type.ROOT));
    assertThat(pathElementsWithExpression.get(1).getType(), equalTo(PathElement.Type.MAP));
    assertThat(pathElementsWithExpression.get(2).getType(), equalTo(PathElement.Type.FIELD_EXPRESSION));
    assertThat(pathElementsWithExpression.get(2).getName(), equalTo(elExpression));
  }
}
