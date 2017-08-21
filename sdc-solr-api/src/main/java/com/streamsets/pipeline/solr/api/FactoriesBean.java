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
package com.streamsets.pipeline.solr.api;

import com.streamsets.pipeline.api.impl.Utils;

import java.util.ServiceLoader;

public abstract class FactoriesBean {

  public abstract SdcSolrTargetFactory createSdcSolrTargetFactory();

  private static ServiceLoader<FactoriesBean> factoriesBeanLoader = ServiceLoader.load(FactoriesBean.class);

  private static FactoriesBean factoriesBean;

  static {
    int serviceCount = 0;
    for (FactoriesBean bean : factoriesBeanLoader) {
      factoriesBean = bean;
      serviceCount++;
    }
    if (serviceCount != 1) {
      throw new RuntimeException(Utils.format("Unexpected number of FactoriesBean: {} instead of 1", serviceCount));
    }
  }

  public static SdcSolrTargetFactory getSolrTargetFactory() {
    return factoriesBean.createSdcSolrTargetFactory();
  }
}
