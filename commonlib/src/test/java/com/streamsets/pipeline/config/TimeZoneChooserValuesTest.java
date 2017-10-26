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
package com.streamsets.pipeline.config;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;

public class TimeZoneChooserValuesTest {

  @Test
  public void getLabelForTimeZoneId() {
    ZoneId zoneId = ZoneId.of("America/Los_Angeles");
    Assert.assertEquals("-07:00 PT (America/Los_Angeles)", TimeZoneChooserValues.getLabelForTimeZoneId(zoneId));
  }
}
