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
package com.streamsets.datacollector.el;

import com.streamsets.datacollector.http.WebServerTask;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ElConstant;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class RuntimeEL {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeEL.class);
  private static final String SDC_PROPERTIES = "sdc.properties";
  private static final String RUNTIME_CONF_LOCATION_KEY = "runtime.conf.location";
  private static final String RUNTIME_CONF_LOCATION_DEFAULT = "embedded";
  private static final String RUNTIME_CONF_PREFIX = "runtime.conf_";
  private static Properties RUNTIME_CONF_PROPS = null;
  private static String AUTH_TOKEN = null;
  private static String HOSTNAME = null;
  private static RuntimeInfo runtimeInfo;

  @ElConstant(name = "NULL", description = "NULL value")
  public static final Object NULL = null;

  private RuntimeEL() {}

  @ElFunction(
    prefix = "runtime",
    name = "conf",
    description = "Retrieves the value of the config property from sdc runtime configuration")
  public static String conf(
    @ElParam("conf") String conf) throws IOException {
    String value = null;
    if(RUNTIME_CONF_PROPS != null) {
      value = RUNTIME_CONF_PROPS.getProperty(conf);
    }
    if(value == null) {
      //Returning a null value instead of throwing an exception results in coercion of the value to the expected
      //return type. This leads to counter intuitive validation error messages.
      throw new IllegalArgumentException(Utils.format("Could not resolve property '{}'", conf).toString());
    }
    return value;
  }

  @ElFunction(
      prefix = "runtime", name = "loadResource",
      description = "Loads the contents of a file under the Data Collector resources directory. " +
                    "If restricted is set to 'true', the file must be readable only by its owner."
  )
  public static String loadResource(
      @ElParam("fileName") String fileName,
      @ElParam("restricted") boolean restricted
  ) {
    String r = loadResourceRaw(fileName, restricted);
    if (r != null) {
      return r.trim();
    }

    return null;
  }

  @ElFunction(
      prefix = "runtime",
      name = "loadResourceRaw",
      description = "Loads the contents of a file including any leading and trailing whitespace under the Data " +
          "Collector resources directory. If restricted is set to 'true', the file must be readable only by its " +
          "owner."
  )
  public static String loadResourceRaw(
      @ElParam("fileName") String fileName,
      @ElParam("restricted") boolean restricted
  ) {
    String resource = null;
    try {
      if (fileName != null && !fileName.isEmpty()) {
        File file = new File(runtimeInfo.getResourcesDir(), fileName);
        if (file.exists() && file.isFile() && restricted) {
          Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file.toPath());
          if (permissions.contains(PosixFilePermission.GROUP_READ) ||
              permissions.contains(PosixFilePermission.OTHERS_READ) ||
              permissions.contains(PosixFilePermission.GROUP_WRITE) ||
              permissions.contains(PosixFilePermission.OTHERS_WRITE)) {
            throw new IllegalArgumentException(Utils.format("File '{}' should be owner read/write only", file));
          }
        }
        byte[] bytes = Files.readAllBytes(file.toPath());
        resource = new String(bytes, StandardCharsets.UTF_8);
      }
    } catch (IllegalArgumentException ex) {
      throw ex;
    } catch (Exception ex) {
      LOG.warn("Could not load resource '{}': {}", fileName, ex.toString(), ex);
    }
    return resource;
  }

  public static void loadRuntimeConfiguration(RuntimeInfo runtimeInfo) throws IOException {
    RuntimeEL.runtimeInfo = runtimeInfo;

    /*

    The sdc.properties file has a property 'runtime.conf.location' with possible values - 'embedded' or 'properties
    file name'.
    If the value is 'embedded', the runtime configuration properties will be read from the sdc.properties,
    from all properties prefixed with 'runtime.conf_'.
    The property names will be trimmed out of the 'runtime.conf_' prefix.

    If the value is a properties file, the file will be looked up in etc/ and loaded as the runtime conf properties,
    no property trimming there.

     */

    Configuration configuration = new Configuration();
    File configFile = new File(runtimeInfo.getConfigDir(), SDC_PROPERTIES);
    if (configFile.exists()) {
      try(FileReader reader = new FileReader(configFile)) {
        configuration.load(reader);
      } catch (IOException ex) {
        LOG.error("Error loading configuration file {} : {}", configFile, ex.getMessage());
        throw new RuntimeException(ex);
      }

      AUTH_TOKEN = runtimeInfo.getAppAuthToken();
      HOSTNAME = configuration.get(WebServerTask.HTTP_BIND_HOST, InetAddress.getLocalHost().getCanonicalHostName());

      RUNTIME_CONF_PROPS = new Properties();
      String runtimeConfLocation = configuration.get(RUNTIME_CONF_LOCATION_KEY, RUNTIME_CONF_LOCATION_DEFAULT);
      if(runtimeConfLocation.equals(RUNTIME_CONF_LOCATION_DEFAULT)) {
        //runtime configuration is embedded in sdc.properties file. Find, trim, cache.
        for(String confName : configuration.getNames()) {
          if(confName.startsWith(RUNTIME_CONF_PREFIX)) {
            RUNTIME_CONF_PROPS.put(confName.substring(RUNTIME_CONF_PREFIX.length()).trim(),
              configuration.get(confName, null));
          }
        }
      } else {
        File runtimeConfFile = new File(runtimeInfo.getConfigDir(), runtimeConfLocation);
        try (FileInputStream fileInputStream = new FileInputStream(runtimeConfFile)) {
          RUNTIME_CONF_PROPS.load(fileInputStream);
        } catch (IOException e) {
          LOG.error("Could not read '{}': {}", runtimeConfFile.getAbsolutePath(), e.toString(), e);
          throw e;
        }
      }
    } else {
      LOG.error("Error did not find sdc.properties at expected location: {}", configFile);
    }
  }

  @ElFunction(
    prefix = "sdc",
    name = "authToken",
    description = "Returns the auth token of this data collector")
  public static String authToken() {
    return AUTH_TOKEN;
  }

  public static Set<Object> getRuntimeConfKeys() {
    return (RUNTIME_CONF_PROPS != null) ? RUNTIME_CONF_PROPS.keySet() : Collections.emptySet();
  }

  @ElFunction(
    prefix = "sdc",
    name = "hostname",
    description = "Return hostname where SDC runs")
  public static String hostname() throws UnknownHostException {
    return HOSTNAME;
  }

  @ElFunction(
      prefix = "runtime",
      name = "availableProcessors",
      description = "Returns the number of CPU cores as reported by Java"
  )
  public static int availableProcessors() {
    return Runtime.getRuntime().availableProcessors();
  }
}
