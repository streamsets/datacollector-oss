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
package com.streamsets.datacollector.execution.dagger;

import com.streamsets.datacollector.http.AsterContext;
import com.streamsets.lib.security.http.aster.AsterService;
import dagger.Module;
import dagger.Provides;
import org.eclipse.jetty.security.Authenticator;

import javax.inject.Singleton;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

@Module(injects = {AsterContext.class}, library = true, complete = false)
public class AsterModuleForTest {

  @Provides
  @Singleton
  public AsterContext provideAsterContext() {
    return new AsterContext() {
      @Override
      public boolean isEnabled() {
        return false;
      }

      @Override
      public AsterService getService() {
        return null;
      }

      @Override
      public void handleRegistration(ServletRequest req, ServletResponse res) {
      }

      @Override
      public Authenticator getAuthenticator() {
        return null;
      }
    };
  }

}
