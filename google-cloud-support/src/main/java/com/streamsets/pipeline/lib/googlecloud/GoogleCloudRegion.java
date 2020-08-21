/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.googlecloud;

import com.streamsets.pipeline.api.Label;

public enum GoogleCloudRegion implements Label {
  US_WEST_1("us-west1"),
  US_WEST_2("us-west2"),
  US_WEST_3("us-west3"),
  US_EAST_1("us-east1"),
  US_EAST_4("us-east4"),
  US_CENTRAL_1("us-central1"),
  ASIA_EAST_1("asia-east1"),
  ASIA_EAST_2("asia-east2"),
  ASIA_NORTH_EAST_1("asia-northeast1"),
  ASIA_NORTH_EAST_2("asia-northeast2"),
  ASIA_NORTH_EAST_3("asia-northeast3"),
  ASIA_SOUTH_1("asia-south1"),
  ASIA_SOUTH_EAST_1("asia-southeast1"),
  AUSTRALIA_SOUTH_EAST_1("australia-southeast1"),
  EUROPE_NORTH_1("europe-north1"),
  EUROPE_WEST_1("europe-west1"),
  EUROPE_WEST_2("europe-west2"),
  EUROPE_WEST_3("europe-west3"),
  EUROPE_WEST_4("europe-west4"), // There is no EUROPE_WEST_5
  EUROPE_WEST_6("europe-west6"),
  NORTH_AMERICA_NORTH_EAST_1("northamerica-northeast1"),
  SOUTH_AMERICA_EAST_1("southamerica-east1"),
  CUSTOM("Custom")
  ;

  private final String label;

  GoogleCloudRegion(String label) {
    this.label = label;
  }
  @Override
  public String getLabel() {
    return label;
  }

}
