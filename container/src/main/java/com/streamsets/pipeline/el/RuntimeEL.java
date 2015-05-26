/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

public class RuntimeEL {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeEL.class);
  private static final String SDC_PROPERTIES = "sdc.properties";
  private static final String RUNTIME_CONF_LOCATION_KEY = "runtime.conf.location";
  private static final String RUNTIME_CONF_LOCATION_DEFAULT = "embedded";
  private static final String RUNTIME_CONF_PREFIX = "runtime.conf_";
  private static Properties RUNTIME_CONF_PROPS = null;

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

  public static void loadRuntimeConfiguration(RuntimeInfo runtimeInfo) throws IOException {
    /*

    The sdc.properties file has a property 'runtime.conf.location' with possible values - 'embedded' or 'properties
    file name'.
    If the value is 'embedded', the runtime configuration properties will be read from the sdc.properties,
    from all properties prefixed with 'runtime.conf_'.
    The property names will be trimmed out of the 'runtime.conf_' prefix.

    If the value is a properties file, the file will be looked up in etc/ and loaded as the runtime conf properties,
    no property trimming there.

     */

    Properties sdcProps = new Properties();
    try(InputStream is = new FileInputStream(new File(runtimeInfo.getConfigDir(), SDC_PROPERTIES))) {
      sdcProps.load(is);
    } catch (IOException e) {
      LOG.error("Could not read '{}' from classpath: {}", SDC_PROPERTIES, e.toString(), e);
    }

    RUNTIME_CONF_PROPS = new Properties();
    String runtimeConfLocation = sdcProps.getProperty(RUNTIME_CONF_LOCATION_KEY, RUNTIME_CONF_LOCATION_DEFAULT);
    if(runtimeConfLocation.equals(RUNTIME_CONF_LOCATION_DEFAULT)) {
      //runtime configuration is embedded in sdc.properties file. Find, trim, cache.
      for(String confName : sdcProps.stringPropertyNames()) {
        if(confName.startsWith(RUNTIME_CONF_PREFIX)) {
          RUNTIME_CONF_PROPS.put(confName.substring(RUNTIME_CONF_PREFIX.length()).trim(),
            sdcProps.getProperty(confName));
        }
      }
    } else {
      try (FileInputStream fileInputStream = new FileInputStream(runtimeConfLocation)) {
        RUNTIME_CONF_PROPS.load(fileInputStream);
      } catch (IOException e) {
        LOG.error("Could not read '{}'", runtimeConfLocation, e.toString(), e);
        throw e;
      }
    }
  }

  public static Set<Object> getRuntimeConfKeys() {
    return RUNTIME_CONF_PROPS.keySet();
  }

}
