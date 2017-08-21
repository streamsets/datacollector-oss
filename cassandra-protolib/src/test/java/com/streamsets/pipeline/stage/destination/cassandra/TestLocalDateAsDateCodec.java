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
package com.streamsets.pipeline.stage.destination.cassandra;

import com.datastax.driver.core.LocalDate;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class TestLocalDateAsDateCodec {
  private static final long millisSinceEpoch = 1490049235015L;
  private static final long expectedMillis = 1489968000000L; // Time portion is zero-ed out
  private static final LocalDate localDate = LocalDate.fromMillisSinceEpoch(millisSinceEpoch);
  private static final Date date = new Date(millisSinceEpoch);

  public LocalDateAsDateCodec codec;

  @Before
  public void setUp() {
    codec = new LocalDateAsDateCodec();
  }

  @Test
  public void deserialize() throws Exception {
    assertEquals(expectedMillis, codec.deserialize(localDate).getTime());
  }

  @Test
  public void serialize() throws Exception {
    assertEquals(expectedMillis, codec.serialize(date).getMillisSinceEpoch());
  }

}
