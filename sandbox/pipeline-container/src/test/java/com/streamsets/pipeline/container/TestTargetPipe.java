/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.container;

import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.api.Target;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;

public class TestTargetPipe {

  @Test(expected = NullPointerException.class)
  public void testInvalidConstructor() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("name");
    Set<String> input = new HashSet<String>();
    input.add("a");
    new TargetPipe(info, null, input);
  }

  @Test
  public void testConstructor() {
    Module.Info info = Mockito.mock(Module.Info.class);
    Mockito.when(info.getInstance()).thenReturn("name");
    Target Target = Mockito.mock(Target.class);
    Set<String> input = new HashSet<String>();
    input.add("a");
    new TargetPipe(info, Target, input);
  }

}
