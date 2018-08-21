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
package com.streamsets.service.parser;

import com.streamsets.pipeline.config.DataFormat;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class DataFormatChooserValuesTest {

  // Make sure that all available data formats are in fact present in the enum that is exposed in user configuration.
  @Test
  public void ensureAllFormatsAreAvailable() {
    List<String> values = new DataFormatChooserValues().getValues();

    for(DataFormat format: DataFormat.values()) {
      // Skip formats for which we don't have parser
      if(format.getParserFormat() == null) {
        return;
      }
      assertTrue("Missing: " + format, values.contains(format.name()));
    }
  }
}
