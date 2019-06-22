/*
 * Copyright 2019 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file
 */
package com.streamsets.pipeline.stage.destination.lib;

import com.streamsets.pipeline.api.Label;
import org.apache.commons.csv.QuoteMode;

public enum DelimitedQuoteMode implements Label {
  ALL("Quote all fields", QuoteMode.ALL),
  MINIMAL("Quote only fields that contain special characters", QuoteMode.MINIMAL),
  NONE("Never quote", QuoteMode.NONE),
  ;

  private String label;
  private QuoteMode quoteMode;

  DelimitedQuoteMode(String label, QuoteMode quoteMode) {
    this.label = label;
    this.quoteMode = quoteMode;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public QuoteMode getQuoteMode() {
    return quoteMode;
  }
}
