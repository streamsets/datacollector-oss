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

import com.streamsets.pipeline.api.Label;

import java.math.BigDecimal;

public enum DecimalScaleRoundingStrategy implements Label {
  //These options as per: https://docs.oracle.com/javase/7/docs/api/java/math/BigDecimal.html
  ROUND_UP("Round up", BigDecimal.ROUND_UP),
  ROUND_DOWN("Round Down", BigDecimal.ROUND_DOWN),
  ROUND_CEILING("Round Ceil", BigDecimal.ROUND_CEILING),
  ROUND_FLOOR("Round Floor", BigDecimal.ROUND_FLOOR),
  ROUND_HALF_UP("Round Half Up", BigDecimal.ROUND_HALF_UP),
  ROUND_HALF_DOWN("Round Half Down", BigDecimal.ROUND_HALF_DOWN),
  ROUND_HALF_EVEN("Round Half Even", BigDecimal.ROUND_HALF_EVEN),
  ROUND_UNNECESSARY("Round Unnecessary", BigDecimal.ROUND_UNNECESSARY),
  ;

  private String label;
  private int roundingStrategy;

  DecimalScaleRoundingStrategy(String label, int roundingStrategy) {
    this.label = label;
    this.roundingStrategy = roundingStrategy;
  }

  public String getLabel() {
    return this.label;
  }

  public int getRoundingStrategy() {
    return this.roundingStrategy;
  }
}
