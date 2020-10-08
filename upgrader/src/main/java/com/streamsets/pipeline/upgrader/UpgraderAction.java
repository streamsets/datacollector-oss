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
package com.streamsets.pipeline.upgrader;

import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.api.el.ELEvalException;
import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public abstract class UpgraderAction<U extends UpgraderAction, T> {
  private static final Logger LOG = LoggerFactory.getLogger(UpgraderAction.class);

  private final static ExpressionEvaluatorImpl EVALUATOR = new ExpressionEvaluatorImpl();


  private static ThreadLocal<Map<String, Object>> CONFIGS_TL = new ThreadLocal<>();

  public static Object valueElFunction(String configName) {
    return CONFIGS_TL.get().get(configName);
  }

  private static Method CONFIG_VALUE_FUNCTION = null;

  protected static final String MATCHES_ALL = ".*";

  static {
    try {
      CONFIG_VALUE_FUNCTION = UpgraderAction.class.getMethod("valueElFunction", String.class);
    } catch (Exception ex) {
      throw new RuntimeException("Should never happen: " + ex.toString(), ex);
    }
  }

  private String evaluate (final Map<String, Object> configs, String expression) throws
      ELEvalException {
    try {
      CONFIGS_TL.set(configs);
      return (String) EVALUATOR.evaluate(
          expression,
          String.class,
          name -> null,
          (prefix, name) -> (prefix.equals("") && name.equals("value")) ? CONFIG_VALUE_FUNCTION : null
      );
    } catch (ELException e) {
      // Apache evaluator is not using the getCause exception chaining that is available in Java but rather a custom
      // chaining mechanism. This doesn't work well for us as we're effectively swallowing the cause that is not
      // available in log, ...
      Throwable t = e;
      if(e.getRootCause() != null) {
        t = e.getRootCause();
        if(e.getCause() == null) {
          e.initCause(t);
        }
      }
      LOG.debug(Errors.YAML_UPGRADER_12.getMessage(), getName(), expression, t.toString(), e);
      throw new ELEvalException(Errors.YAML_UPGRADER_12, getName(), expression, t.toString());
    } finally {
      CONFIGS_TL.remove();
    }
  }



  protected interface ConfigsAdapter extends Iterable<ConfigsAdapter.Pair> {
    interface Pair {
      String getName();
      Object getValue();
    }

    ConfigsAdapter.Pair find(String name);
    ConfigsAdapter.Pair set(String name, Object value);
    ConfigsAdapter.Pair remove(String name);

    @NotNull
    @Override
    Iterator<Pair> iterator();

    Map<String, Object> toConfigMap();
  }

  private static class ListConfigConfigsAdapter implements ConfigsAdapter {

    private static class PairImpl implements Pair {
      private final Config config;

      public PairImpl(Config config) {
        this.config = config;
      }

      @Override
      public String getName() {
        return config.getName();
      }

      @Override
      public Object getValue() {
        return config.getValue();
      }

    }

    private final List<Config> configs;

    public ListConfigConfigsAdapter(Object configs) {
      this.configs = (List<Config>) configs;
    }

    protected int findIndexOf(List<Config> configs, String name) {
      for (int i = 0; i < configs.size(); i++) {
        if (name.equals(configs.get(i).getName())) {
          return i;
        }
      }
      return -1;
    }

    @Override
    public ConfigsAdapter.Pair find(String name) {
      int configIdx = findIndexOf(configs, name);
      return (configIdx == -1) ? null : new PairImpl(configs.get(configIdx));
    }

    @Override
    public ConfigsAdapter.Pair set(String name, Object value) {
      int configIdx = findIndexOf(configs, name);
      Config config = new Config(name, value);
      if (configIdx == -1) {
        configs.add(config);
      } else {
        configs.set(configIdx, config);
      }
      return new PairImpl(config);
    }

    @Override
    public ConfigsAdapter.Pair remove(String name) {
      int configIdx = findIndexOf(configs, name);
      Config config = (configIdx == -1) ? null : configs.remove(configIdx);
      return (config == null) ? null : new PairImpl(config);
    }

    @NotNull
    @Override
    public Iterator<Pair> iterator() {
      return configs.stream().map(c -> (ConfigsAdapter.Pair) new PairImpl(c)).collect(Collectors.toList()).iterator();
    }

    @Override
    public Map<String, Object> toConfigMap() {
      // Using for loop instead of streams because Collectors.toMap() throws NPE when value is null (JDK-8148463)
      Map<String, Object> map = new HashMap<>();
      for (Config config : configs) {
        map.put(config.getName(), config.getValue());
      }
      return map;
    }

  }

  private static class MapConfigsAdapter implements ConfigsAdapter {

    private static class PairImpl implements Pair {
      private final String name;
      private final Object value;

      public PairImpl(String name, Object value) {
        this.name = name;
        this.value = value;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public Object getValue() {
        return value;
      }

    }

    private final Map<String, Object> configs;

    public MapConfigsAdapter(Object configs) {
      this.configs = (Map<String, Object>) configs;
    }

    @Override
    public ConfigsAdapter.Pair find(String name) {
      Object value = configs.get(name);
      return (configs.containsKey(name)) ? new PairImpl(name, value) : null;
    }

    @Override
    public ConfigsAdapter.Pair set(String name, Object value) {
      configs.put(name, value);
      return new PairImpl(name, value);
    }

    @Override
    public ConfigsAdapter.Pair remove(String name) {
      Object value = configs.remove(name);
      return (configs.containsKey(name)) ? new PairImpl(name, value) : null;
    }

    @NotNull
    @Override
    public Iterator<Pair> iterator() {
      return configs.entrySet()
          .stream()
          .map(e -> (ConfigsAdapter.Pair) new PairImpl(e.getKey(), e.getValue()))
          .collect(Collectors.toList())
          .iterator();
    }

    @Override
    public Map<String, Object> toConfigMap() {
      return configs;
    }

  }

  public static final Function<List<Config>, ConfigsAdapter> CONFIGS_WRAPPER = ListConfigConfigsAdapter::new;

  public static final Function<Map<String, Object>, ConfigsAdapter> MAP_WRAPPER = MapConfigsAdapter::new;

  private final Function<T, ConfigsAdapter> wrapper;
  private String name;

  public UpgraderAction(Function<T, ConfigsAdapter> wrapper) {
    this.wrapper = wrapper;
  }

  protected ConfigsAdapter wrap(T configs) {
    return wrapper.apply(configs);
  }

  protected Object resolveValueIfEL(Map<String, Object> originalConfigs, Object value) {
    if (value instanceof String) {
      value = evaluate(originalConfigs, (String) value);
    }
    return value;
  }

  public abstract void upgrade(StageUpgrader.Context context, Map<String, Object> originalConfigs, T configs);

  public String getName() {
    return name;
  }

  public U setName(String name) {
    this.name = name;
    return (U) this;
  }

  /**
   * Check if a configuration exists and has a given value.  If the configuration value is null, it'll match against
   * empty string.
   *
   * @param name    Name of the configuration to look for.
   * @param value   Expected value for the configuration. Use {@value MATCHES_ALL} to accept any value.
   * @param adapter Adapter used to find the configuration.
   * @return true if {@code name} exists and equals {@code value}, false otherwise.
   */
  protected boolean existsConfigWithValue(String name, Object value, ConfigsAdapter adapter) {
    boolean exists = false;
    ConfigsAdapter.Pair config = adapter.find(name);
    if (config != null) {
      if (MATCHES_ALL.equals(value)) {
        exists = true;
      } else {
        boolean matches;
        Object configValue = config.getValue() != null ? config.getValue() : "";
        if ((configValue instanceof String)) {
          Pattern pattern = Pattern.compile(value.toString());
          matches = pattern.matcher(configValue.toString()).matches();
        } else {
          matches = configValue.equals(value);
        }
        exists = matches;
      }
    }
    return exists;
  }

}
