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
package com.streamsets.datacollector.aster;

import com.streamsets.datacollector.http.AsterConfig;
import com.streamsets.datacollector.http.AsterContext;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.aster.AsterService;
import com.streamsets.pipeline.SDCClassLoader;
import dagger.Module;
import dagger.Provides;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

@Module(injects = {AsterContext.class}, library = true, complete = false)
public class AsterModule {
  private static final Logger LOG = LoggerFactory.getLogger(AsterModule.class);


  @Provides
  @Singleton
  public AsterContext provideAsterContext(BuildInfo buildInfo, RuntimeInfo runtimeInfo, Configuration config) {
    LOG.debug("Initializing Aster Service");
    ConstraintSecurityHandler security = new ConstraintSecurityHandler();

    // creates the configuration bean with the information needed to create the Aster authenticator.
    AsterConfig asterConfig = new AsterConfig(
        runtimeInfo.getProductName().equals(RuntimeInfo.SDC_PRODUCT) ? "DC" : "TF",
        buildInfo.getVersion(),
        runtimeInfo.getId(),
        config,
        runtimeInfo.getDataDir()
    );

    try {
      // gets JAR URLs from Aster client lib directory
      List<URL> asterJars = AsterUtil.getAsterJars(runtimeInfo);

      // creates a reverse classloader (the same one we use for stage libraries) with the Aster client JARs
      // using the container classloader as parent
      SDCClassLoader asterClassLoader = SDCClassLoader.getAsterClassLoader(
          asterJars,
          Thread.currentThread().getContextClassLoader()
      );

      // Lookup for the AsterContextCreator function class in the Aster client classloader.
      Class<BiFunction<RuntimeInfo, AsterConfig, AsterContext>> klass = (Class<BiFunction<RuntimeInfo, AsterConfig, AsterContext>>)
              asterClassLoader.loadClass("com.streamsets.lib.security.http.aster.AsterContextCreator");

      // Instantiate and invoke the AsterContextCreator function providing the Aster configuration bean.
      BiFunction<RuntimeInfo, AsterConfig, AsterContext> creator = klass.newInstance();
      return creator.apply(runtimeInfo, asterConfig);
    } catch (Exception ex) {
      throw new RuntimeException("Could not create AsterContext: " + ex, ex);
    }
  }

}
