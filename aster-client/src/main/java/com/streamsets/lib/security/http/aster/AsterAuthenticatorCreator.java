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

import com.streamsets.datacollector.http.AsterAuthenticatorConfig;
import org.eclipse.jetty.security.Authenticator;

import java.io.File;
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
public class AsterAuthenticatorCreator implements Function<AsterAuthenticatorConfig, Authenticator> {

  private Authenticator create(AsterAuthenticatorConfig asterAuthenticatorConfig) {
    return ClassLoaderUtils.withOwnClassLoader(getClass().getClassLoader(), () -> {
      AsterServiceConfig serviceConfig = new AsterServiceConfig(
          AsterRestConfig.SubjectType.valueOf(asterAuthenticatorConfig.getEngineType()),
          asterAuthenticatorConfig.getEngineVersion(),
          asterAuthenticatorConfig.getEngineId(),
          asterAuthenticatorConfig.getEngineConf()
      );
      File tokensFile = new File(asterAuthenticatorConfig.getDataDir(), "aster-token.json");
      AsterServiceImpl service = new AsterServiceImpl(serviceConfig, tokensFile);
      AsterServiceProvider.getInstance().set(service);
      return new AsterAuthenticator(service);
    });
  }

  /**
   * Creates an Aster {@link Authenticator} wrapped with a {@link ClassLoaderInContextAuthenticator}.
   */
  @Override
  public Authenticator apply(AsterAuthenticatorConfig asterAuthenticatorConfig) {
    return new ClassLoaderInContextAuthenticator(create(asterAuthenticatorConfig));
  }

}
