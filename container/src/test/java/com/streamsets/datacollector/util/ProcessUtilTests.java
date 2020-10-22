/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.util;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProcessUtilTests {

  @Test
  public void testSuccess() {
    boolean returnValue = ProcessUtil.executeCommand(ImmutableList.of("echo", "1"), 2, (out, err) -> {
    });
    assertTrue(returnValue);
  }

  @Test
  public void testFailure() {
    boolean returnValue = ProcessUtil.executeCommand(ImmutableList.of("idonotexists"), 2, (out, err) -> {
    });
    assertFalse(returnValue);
  }

  @Test
  public void testTimeout() {
    boolean returnValue = ProcessUtil.executeCommand(ImmutableList.of("sleep", "15"), 1, (out, err) -> {
    });
    assertFalse(returnValue);
  }

  @Test
  public void testStdOut() {
    StringBuilder builder = new StringBuilder();

    boolean returnValue = ProcessUtil.executeCommand(ImmutableList.of("echo", "1"), 1, (out, err) -> {
      builder.append(Files.toString(out.toFile(), Charset.defaultCharset()));
    });

    assertTrue(returnValue);
    assertEquals("1\n", builder.toString());
  }

  @Test
  public void testLoadOutputSuccess() {
    ProcessUtil.Output output = ProcessUtil.executeCommandAndLoadOutput(ImmutableList.of("echo", "1"), 2);
    assertTrue(output.success);
    assertEquals(output.stdout, "1\n");
    assertEquals(output.stderr, "");
  }

  @Test
  public void testLoadOutputFailure() {
    ProcessUtil.Output output = ProcessUtil.executeCommandAndLoadOutput(ImmutableList.of("idonotexists"), 2);
    assertFalse(output.success);
    assertEquals(output.stdout, "");
    assertEquals(output.stderr, "");
  }
}
