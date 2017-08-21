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
package com.streamsets.datacollector.security;

import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ServiceLoader;

public abstract class LoginUgiProviderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(LoginUgiProviderFactory.class);
  private static final LoginUgiProvider defaultLoginUgiProvider = new DefaultLoginUgiProvider();

  private static LoginUgiProviderFactory loginUgiProviderFactory;

  public abstract LoginUgiProvider createLoginUgiProvider();

  private static ServiceLoader<LoginUgiProviderFactory> loginUgiProviderFactoryServiceLoader =
      ServiceLoader.load(LoginUgiProviderFactory.class);

  static {
    int serviceCount = 0;
    for (LoginUgiProviderFactory bean : loginUgiProviderFactoryServiceLoader) {
      loginUgiProviderFactory = bean;
      serviceCount++;
    }

    if (loginUgiProviderFactory == null) {
      LOG.warn("Could not find implementation of LoginUgiProviderFactory. Using default implementation meant for Apache Hadoop.");
      loginUgiProviderFactory = getDefaultLoginUgiProviderFactory();
    } else if (serviceCount != 1) {
      throw new RuntimeException(
          Utils.format("Unexpected number of LoginUgiProviderFactory : {} instead of 1", serviceCount)
      );
    }
  }

  public static LoginUgiProvider getLoginUgiProvider() {
    return loginUgiProviderFactory.createLoginUgiProvider();
  }

  private static LoginUgiProviderFactory getDefaultLoginUgiProviderFactory() {
    return new LoginUgiProviderFactory() {
      @Override
      public LoginUgiProvider createLoginUgiProvider() {
       return defaultLoginUgiProvider;
      }
    };
  }
}
