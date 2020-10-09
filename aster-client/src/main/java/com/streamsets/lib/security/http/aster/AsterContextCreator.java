/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.lib.security.http.aster;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.http.AsterConfig;
import com.streamsets.datacollector.http.AsterContext;
import com.streamsets.datacollector.main.RuntimeInfo;
import org.eclipse.jetty.security.Authenticator;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.File;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * {@link Function} that creates an Aster {@link Authenticator} using the given configuration.
 * <p/>
 * The authenticator is created with the function class' classloader in context. The returned authenticator
 * is wrapped with {@link ClassLoaderInContextAuthenticator} to ensure all invocations use the {@link ClassLoader} of
 * the Aster authenticator.
 * <p/>
 * The Aster authenticator stores the token credentials in a {@code aster-token.json} file within the specified
 * data directory.
 */
public class AsterContextCreator implements BiFunction<RuntimeInfo, AsterConfig, AsterContext> {

  /**
   * Creates an Aster {@link Authenticator} wrapped with a {@link ClassLoaderInContextAuthenticator}.
   */
  @Override
  public AsterContext apply(RuntimeInfo runtimeInfo, AsterConfig asterConfig) {
    AsterService service;
    String asterUrl = asterConfig.getEngineConf().get(AsterServiceProvider.ASTER_URL, AsterServiceProvider.ASTER_URL_DEFAULT);
    if (!asterUrl.isEmpty()) {
      AsterServiceConfig serviceConfig = new AsterServiceConfig(
          AsterRestConfig.SubjectType.valueOf(asterConfig.getEngineType()),
          asterConfig.getEngineVersion(),
          asterConfig.getEngineId(),
          asterConfig.getEngineConf()
      );
      File tokensFile = new File(asterConfig.getDataDir(), "aster-token.json");
      service = new AsterServiceImpl(serviceConfig, tokensFile);
    } else {
      service = null;
    }

    ClassLoaderInContextAuthenticator authenticator =
        new ClassLoaderInContextAuthenticator(new AsterAuthenticator(service, runtimeInfo));

    return new AsterContext() {
      @Override
      public boolean isEnabled() {
        return service != null;
      }

      @Override
      public AsterService getService() {
        Preconditions.checkState(service != null, "Aster service not available");
        return service;
      }

      @Override
      public void handleRegistration(ServletRequest req, ServletResponse res)  {
        Preconditions.checkState(service != null, "Aster service not available");
        authenticator.handleRegistration(req, res);
      }

      @Override
      public Authenticator getAuthenticator() {
        Preconditions.checkState(service != null, "Aster service not available");
        return authenticator;
      }
    };
  }

}
