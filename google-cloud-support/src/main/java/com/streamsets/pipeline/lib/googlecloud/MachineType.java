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

public enum MachineType implements Label {
  N1_STANDARD_2,
  N1_STANDARD_4,
  N1_STANDARD_8,
  N1_STANDARD_16,
  N1_STANDARD_32,
  N1_STANDARD_64,
  N1_STANDARD_96,
  
  N1_HIGHMEM_2,
  N1_HIGHMEM_4,
  N1_HIGHMEM_8,
  N1_HIGHMEM_16,
  N1_HIGHMEM_32,
  N1_HIGHMEM_64,
  N1_HIGHMEM_96,
  
  N1_HIGHCPU_2,
  N1_HIGHCPU_4,
  N1_HIGHCPU_8,
  N1_HIGHCPU_16,
  N1_HIGHCPU_32,
  N1_HIGHCPU_64,
  N1_HIGHCPU_96,
  
  N2_STANDARD_2,
  N2_STANDARD_4,
  N2_STANDARD_8,
  N2_STANDARD_16,
  N2_STANDARD_32,
  N2_STANDARD_48,
  N2_STANDARD_64,
  N2_STANDARD_80,
  
  N2_HIGHMEM_2,
  N2_HIGHMEM_4,
  N2_HIGHMEM_8,
  N2_HIGHMEM_16,
  N2_HIGHMEM_32,
  N2_HIGHMEM_48,
  N2_HIGHMEM_64,
  N2_HIGHMEM_80,
  
  N2_HIGHCPU_2,
  N2_HIGHCPU_4,
  N2_HIGHCPU_8,
  N2_HIGHCPU_16,
  N2_HIGHCPU_32,
  N2_HIGHCPU_48,
  N2_HIGHCPU_64,
  N2_HIGHCPU_80,
  
  N2D_STANDARD_2,
  N2D_STANDARD_4,
  N2D_STANDARD_8,
  N2D_STANDARD_16,
  N2D_STANDARD_32,
  ND2_STANDARD_48,
  N2D_STANDARD_64,
  N2D_STANDARD_80,
  N2D_STANDARD_96,
  N2D_STANDARD_128,
  N2D_STANDARD_224,
  
  N2D_HIGHMEM_2,
  N2D_HIGHMEM_4,
  N2D_HIGHMEM_8,
  N2D_HIGHMEM_16,
  N2D_HIGHMEM_32,
  N2D_HIGHMEM_48,
  N2D_HIGHMEM_64,
  N2D_HIGHMEM_80,
  N2D_HIGHMEM_96,
  
  N2D_HIGHCPU_2,
  N2D_HIGHCPU_4,
  N2D_HIGHCPU_8,
  N2D_HIGHCPU_16,
  N2D_HIGHCPU_32,
  N2D_HIGHCPU_48,
  N2D_HIGHCPU_64,
  N2D_HIGHCPU_80,
  N2D_HIGHCPU_96,
  N2D_HIGHCPU_128,
  N2D_HIGHCPU_224,
  
  E2_STANDARD_2,
  E2_STANDARD_4,
  E2_STANDARD_8,
  E2_STANDARD_16,
  
  E2_HIGHMEM_2,
  E2_HIGHMEM_4,
  E2_HIGHMEM_8,
  E2_HIGHMEM_16,
  
  E2_HIGHCPU_2,
  E2_HIGHCPU_4,
  E2_HIGHCPU_8,
  E2_HIGHCPU_16,
  
  M1_ULTRAMEM_40,
  M1_ULTRAMEM_80,
  M1_ULTRAMEM_160,
  
  M2_ULTRAMEM_208,
  M2_ULTRAMEM_416,
  
  C2_STANDARD_4,
  C2_STANDARD_8,
  C2_STANDARD_16,
  C2_STANDARD_30,
  C2_STANDARD_60,
  
  CUSTOM
  
  ;
  
  @Override
  public String getLabel() {
    return this.name().replace("_", "-").toLowerCase();
  }
}
