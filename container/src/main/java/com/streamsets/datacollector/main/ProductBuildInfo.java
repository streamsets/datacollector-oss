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
package com.streamsets.datacollector.main;

import com.streamsets.pipeline.BootstrapMain;

public class ProductBuildInfo extends BuildInfo {
  public static final String DEFAULT_PRODUCT_NAME = BootstrapMain.DEFAULT_PRODUCT_NAME;

  private static final String BUILD_INFO_FILE = "%s-buildinfo.properties";

  public ProductBuildInfo(String productName) {
    super(String.format(BUILD_INFO_FILE, productName));
  }

  private static final ProductBuildInfo DEFAULT_INSTANCE = new ProductBuildInfo(DEFAULT_PRODUCT_NAME);

  public static BuildInfo getDefault() {
    return DEFAULT_INSTANCE;
  }
}
