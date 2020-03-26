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
package com.streamsets.pipeline.kafka.api;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FactoriesBeanTest {
  private FactoriesBean kafka09;
  private FactoriesBean kafka10;
  private FactoriesBean kafka11;
  private FactoriesBean mapr09;
  private FactoriesBean mapr52;

  @Before
  public void setUp() {
    this.kafka09 = mock(FactoriesBean.class);
    when(this.kafka09.getClassName()).thenReturn("com.streamsets.pipeline.kafka.impl.Kafka09FactoriesBean");
    this.kafka10 = mock(FactoriesBean.class);
    when(this.kafka10.getClassName()).thenReturn("com.streamsets.pipeline.kafka.impl.Kafka10FactoriesBean");
    this.kafka11 = mock(FactoriesBean.class);
    when(this.kafka11.getClassName()).thenReturn("com.streamsets.pipeline.kafka.impl.Kafka11FactoriesBean");
    this.mapr09 = mock(FactoriesBean.class);
    when(this.mapr09.getClassName()).thenReturn("com.streamsets.pipeline.kafka.impl.MapRStreams09FactoriesBean");
    this.mapr52 = mock(FactoriesBean.class);
    when(this.mapr52.getClassName()).thenReturn("com.streamsets.pipeline.kafka.impl.MapR52Streams09FactoriesBean");
  }

  @Test
  public void runValidCombinations() {
    assertEquals(this.kafka10, FactoriesBean.loadBean(ImmutableList.of(kafka09, kafka10)));
    assertEquals(this.kafka10, FactoriesBean.loadBean(ImmutableList.of(kafka10, kafka09)));

    assertEquals(this.kafka11, FactoriesBean.loadBean(ImmutableList.of(kafka09, kafka11)));
    assertEquals(this.kafka11, FactoriesBean.loadBean(ImmutableList.of(kafka11, kafka09)));

    assertEquals(this.mapr52, FactoriesBean.loadBean(ImmutableList.of(mapr09, mapr52)));
    assertEquals(this.mapr52, FactoriesBean.loadBean(ImmutableList.of(mapr52, mapr09)));
  }

  @Test(expected = RuntimeException.class)
  public void runInvalid11and10() {
    FactoriesBean.loadBean(ImmutableList.of(kafka11, kafka10));
  }
}
