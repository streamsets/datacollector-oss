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
package com.streamsets.datacollector.creation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.ModelType;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.configupgrade.ConnectionConfigurationUpgrader;
import com.streamsets.datacollector.credential.ClearCredentialValue;
import com.streamsets.datacollector.definition.ConfigValueExtractor;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ElUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.IssueCreator;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConnectionDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * General config injector that will work with various object types.
 */
public class ConfigInjector {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineBeanCreator.class);

  private static ConnectionRetriever connectionRetriever;

  private static final ConfigInjector INJECTOR = new ConfigInjector() {
    {
      ///TODO add specific mapper options
    }
  };
  public static ConfigInjector get() {
    return INJECTOR;
  }

  public static void prepareForConnections(Configuration configuration, RuntimeInfo runtimeInfo) {
    prepareForConnections(new ConnectionRetriever(configuration, runtimeInfo));
  }

  @VisibleForTesting
  static void prepareForConnections(ConnectionRetriever connectionRetriever) {
    ConfigInjector.connectionRetriever = connectionRetriever;
  }

  /**
   * Context object containing various events important for the injection.
   */
  public interface Context {

    /**
     * Return ConfigDefinition for given configuration name.
     */
    ConfigDefinition getConfigDefinition(String configName);

    /**
     * Return value for given configuration name or null if it doesn't exists.
     */
    Object getConfigValue(String configName);

    /**
     * Create new issue.
     */
    void createIssue(ErrorCode error, Object... args);

    /**
     * Create new issue.
     */
    void createIssue(String configGroup, String configName, ErrorCode error, Object... args);

    /**
     * Add issues.
     * @param issues
     */
    void addIssues(List<Issue> issues);

    /**
     * Error description to identify the injecting component (like 'Stage "JDBC Origin"'). Will be used
     * in exceptions and logs.
     */
    String errorDescription();

    /**
     * Pipeline constants.
     */
    Map<String, Object> getPipelineConstants();

    /**
     * Connections used by this pipeline.
     */
    Map<String, ConnectionConfiguration> getConnections();

    /**
     * The user who is running this pipeline.
     */
    String getUser();
  }

  /**
   * Internal implementation of Context that can wrap parent and override configuration definition and values. Primarily
   * needed when injecting complex fields.
   */
  private static class OverrideContext implements Context {
    private final Context parent;
    Map<String, Object> values;
    Map<String, ConfigDefinition> definitions;

    public OverrideContext(Context parent, Map<String, Object> values, Map<String, ConfigDefinition> definitions) {
      this.parent = parent;
      this.values = values;
      this.definitions = definitions;
    }

    @Override
    public ConfigDefinition getConfigDefinition(String configName) {
      return definitions.get(configName);
    }

    @Override
    public Object getConfigValue(String configName) {
      return values.get(configName);
    }

    @Override
    public void createIssue(ErrorCode error, Object... args) {
      parent.createIssue(error, args);
    }

    @Override
    public void createIssue(String configGroup, String configName, ErrorCode error, Object... args) {
      parent.createIssue(configGroup, configName, error, args);
    }

    @Override
    public void addIssues(List<Issue> issues) {
      parent.addIssues(issues);
    }

    @Override
    public String errorDescription() {
      return parent.errorDescription();
    }

    @Override
    public Map<String, Object> getPipelineConstants() {
      return parent.getPipelineConstants();
    }

    @Override
    public Map<String, ConnectionConfiguration> getConnections() {
      return parent.getConnections();
    }

    @Override
    public String getUser() {
      return parent.getUser();
    }
  }

  /**
   * Context object when ingesting a Stage object.
   */
  public static class StageInjectorContext implements Context {

    private final StageDefinition definition;
    private final StageConfiguration configuration;
    private final IssueCreator issueCreator;
    private final Map<String, Object> pipelineConstants;
    private final String user;
    private final Map<String, ConnectionConfiguration> connections;
    private final List<Issue> issues;

    public StageInjectorContext(
        StageDefinition definition,
        StageConfiguration configuration,
        Map<String, Object> pipelineConstants,
        String user,
        Map<String, ConnectionConfiguration> connections,
        List<Issue> issues
    ) {
      this.definition = definition;
      this.configuration = configuration;
      this.issueCreator = IssueCreator.getStage(configuration.getInstanceName());
      this.pipelineConstants = pipelineConstants;
      this.user = user;
      this.connections = connections;
      this.issues = issues;
    }

    public StageInjectorContext(
        StageInjectorContext old,
        Map<String, Object> pipelineConstantsOverride
    ) {
      this.definition = old.definition;
      this.configuration = old.configuration;
      this.issueCreator = old.issueCreator;
      this.pipelineConstants = pipelineConstantsOverride;
      this.user = old.user;
      this.connections = old.connections;
      this.issues = old.issues;
    }

    @Override
    public ConfigDefinition getConfigDefinition(String configName) {
      return definition.getConfigDefinition(configName);
    }

    @Override
    public Object getConfigValue(String configName) {
      Config config = configuration.getConfig(configName);

      if(config == null) {
        return null;
      }

      return config.getValue();
    }

    @Override
    public void createIssue(ErrorCode error, Object... args) {
      issues.add(issueCreator.create(error, args));
    }

    @Override
    public void createIssue(String configGroup, String configName, ErrorCode error, Object... args) {
      issues.add(issueCreator.create(configGroup, configName, error, args));
    }

    @Override
    public void addIssues(List<Issue> issues) {
      this.issues.addAll(issues);
    }

    @Override
    public String errorDescription() {
      return Utils.format("Stage '{}'", configuration.getInstanceName());
    }

    @Override
    public Map<String, Object> getPipelineConstants() {
      return pipelineConstants;
    }

    @Override
    public Map<String, ConnectionConfiguration> getConnections() {
      return connections;
    }

    @Override
    public String getUser() {
      return user;
    }
  }

  public static class ConnectionOverlayInjectorContext implements Context {

    private Context parentContext;
    private Map<String, Config> connConfigs;

    public ConnectionOverlayInjectorContext(Context parentContext, String connPrefix, List<Config> connConfigs) {
      this.parentContext = parentContext;
      this.connConfigs = connConfigs.stream().collect(
              Collectors.toMap(config -> connPrefix + config.getName(), config -> config));
    }

    @Override
    public ConfigDefinition getConfigDefinition(String configName) {
      return parentContext.getConfigDefinition(configName);
    }

    @Override
    public Object getConfigValue(String configName) {
      Config config = connConfigs.get(configName);

      if(config == null) {
        return null;
      }

      return config.getValue();
    }

    @Override
    public void createIssue(ErrorCode error, Object... args) {
      parentContext.createIssue(error, args);
    }

    @Override
    public void createIssue(String configGroup, String configName, ErrorCode error, Object... args) {
      parentContext.createIssue(configGroup, configName, error, args);
    }

    @Override
    public void addIssues(List<Issue> issues) {
      parentContext.addIssues(issues);
    }

    @Override
    public String errorDescription() {
      return parentContext.errorDescription();
    }

    @Override
    public Map<String, Object> getPipelineConstants() {
      return parentContext.getPipelineConstants();
    }

    @Override
    public Map<String, ConnectionConfiguration> getConnections() {
      return parentContext.getConnections();
    }

    @Override
    public String getUser() {
      return parentContext.getUser();
    }
  }

  public static class ServiceInjectorContext implements Context {

    private final String stageName;
    private final ServiceDefinition definition;
    private final ServiceConfiguration configuration;
    private final Map<String, Object> pipelineConstants;
    private final String user;
    private final Map<String, ConnectionConfiguration> connections;
    private final IssueCreator issueCreator;
    private final List<Issue> issues;

    public ServiceInjectorContext(
        String stageName,
        ServiceDefinition definition,
        ServiceConfiguration configuration,
        Map<String, Object> pipelineConstants,
        String user,
        Map<String, ConnectionConfiguration> connections,
        List<Issue> issues
    ) {
      this.stageName = stageName;
      this.definition = definition;
      this.configuration = configuration;
      this.pipelineConstants = pipelineConstants;
      this.user = user;
      this.connections = connections;
      this.issueCreator = IssueCreator.getService(stageName, definition.getKlass().getName());
      this.issues = issues;
    }

    @Override
    public ConfigDefinition getConfigDefinition(String configName) {
      return definition.getConfigDefinitionsMap().get(configName);
    }

    @Override
    public Object getConfigValue(String configName) {
      Config config = configuration.getConfig(configName);

      if(config == null) {
        return null;
      }

      return config.getValue();
    }

    @Override
    public void createIssue(ErrorCode error, Object... args) {
      issues.add(issueCreator.create(error, args));
    }

    @Override
    public void createIssue(String configGroup, String configName, ErrorCode error, Object... args) {
      issues.add(issueCreator.create(configGroup, configName, error, args));
    }

    @Override
    public void addIssues(List<Issue> issues) {
      this.issues.addAll(issues);
    }

    @Override
    public String errorDescription() {
      return Utils.format("Stage '{}' Service '{}'", stageName, definition.getKlass().getName());
    }

    @Override
    public Map<String, Object> getPipelineConstants() {
      return pipelineConstants;
    }

    @Override
    public Map<String, ConnectionConfiguration> getConnections() {
      return connections;
    }

    @Override
    public String getUser() {
      return user;
    }
  }

  /**
   * Inject config values to given Stage.
   *
   * @param stage Stage instance
   * @param stageDef Definition for given stage
   * @param stageConf Actual configuration values
   * @param constants Pipeline constants (parameters)
   * @param user The user
   * @param issues List into which issues will be added
   */
  public void injectStage(
      Object stage,
      StageDefinition stageDef,
      StageConfiguration stageConf,
      Map<String, Object> constants,
      String user,
      Map<String, ConnectionConfiguration> connections,
      List<Issue> issues
  ) {
    injectConfigsToObject(stage, new StageInjectorContext(stageDef, stageConf, constants, user, connections, issues));
  }

  public void injectService(
      Object service,
      String stageName,
      ServiceDefinition def,
      ServiceConfiguration conf,
      Map<String, Object> constants,
      String user,
      Map<String, ConnectionConfiguration> connections,
      List<Issue> issues
  ) {
    injectConfigsToObject(service, new ServiceInjectorContext(stageName, def, conf, constants, user, connections, issues));
  }

  void injectConfigsToObject(Object object, Context context) {
    if (createConfigBeans(object, "", context)) {
      injectConfigs(object, "", context);
    }
  }

  public boolean createConfigBeans(Object obj, String configPrefix, Context context) {
    boolean ok = true;
    Class klass = obj.getClass();
    for (Field field : klass.getFields()) {
      String configName = configPrefix + field.getName();
      if (field.getAnnotation(ConfigDefBean.class) != null) {
        try {
          Object bean = field.getType().newInstance();
          ok &= createConfigBeans(bean, configName + ".", context);
          if (ok) {
            field.set(obj, bean);
          }
        } catch (InstantiationException | IllegalAccessException ex) {
          ok = false;
          context.createIssue(CreationError.CREATION_001, field.getType().getSimpleName(), ex.toString());
        }
      // if field is ConfigDef with a non-empty connection type, we need to do extra processing
      } else if (field.getAnnotation(ConfigDef.class) != null
              && StringUtils.isNotEmpty(field.getAnnotation(ConfigDef.class).connectionType())) {
        // if the value of this connectionType config is anything other than 'MANUAL' we assume it's a connection ID
        Object configValue = context.getConfigValue(configName);
        if (context.getUser() != null && context.getConnections() != null && configValue != null
            && !((String) configValue).equalsIgnoreCase(ConnectionDef.Constants.CONNECTION_SELECT_MANUAL)) {
          // add connection to stage injector context to be used later
          String connectionId = (String) configValue;
          ConnectionConfiguration cc = context.getConnections().computeIfAbsent(
              connectionId,
              cid -> connectionRetriever.get(cid, context)
          );
          if (cc == null) {
            ok = false;
            // connectionRetriever created the issue
          }
        }
      }
    }
    return ok;
  }

  public void injectConfigs(Object obj, String configPrefix, Context context) {
    for (Field field : obj.getClass().getFields()) {
      String configName = configPrefix + field.getName();
      if (field.getAnnotation(ConfigDef.class) != null) {
        ConfigDefinition configDef = context.getConfigDefinition(configName);
        // if there is no config def, we ignore it, it can be the case when the config is a @HideConfig
        if (configDef != null) {
          Object value = context.getConfigValue(configName);
          if (value == null) {
            LOG.trace("{} missing configuration '{}', using default", context.errorDescription(), configDef.getName());
            injectDefaultValue(obj, field, configDef, context);
          } else {
            injectConfigValue(obj, field, value, configDef, context);
          }
        }
      } else if (field.getAnnotation(ConfigDefBean.class) != null) {
        boolean injected = false;
        // if ConfigDefBean is also a ConnectionDef, check if we need to handle it
        ConnectionDef connectionDef = field.getType().getAnnotation(ConnectionDef.class);
        if (connectionDef != null) {
          // Figure out the ConnectionId that this ConnectionDef is associated with
          String connectionId = getConnectionId(obj, field, context);
          if (connectionId != null && context.getConnections() != null
              && !connectionId.equalsIgnoreCase(ConnectionDef.Constants.CONNECTION_SELECT_MANUAL)) {
            // We should have populated this earlier
            ConnectionConfiguration cc = context.getConnections().get(connectionId);
            if (cc != null) {
              if (!cc.getType().equals(connectionDef.type())) {
                // Connection type mismatch
                context.createIssue(CreationError.CREATION_1102, cc.getType(), connectionDef.type());
              } else {
                // Do an upgrade if necessary
                List<Issue> issues = new ArrayList<>();
                ConnectionConfigurationUpgrader.get().upgradeIfNecessary(connectionDef, cc, connectionId, issues);
                context.addIssues(issues);
                if (issues.isEmpty()) {
                  // Inject the connection's configs into the stage's configs
                  String connConfigPrefix = configName + ".";
                  Context connContext = new ConnectionOverlayInjectorContext(context,
                      connConfigPrefix,
                      cc.getConfiguration()
                  );
                  try {
                    injectConfigs(field.get(obj), connConfigPrefix, connContext);
                    injected = true;
                  } catch (IllegalArgumentException | IllegalAccessException ex) {
                    context.createIssue(CreationError.CREATION_003, ex.toString());
                  }
                }
              }
            }
          }
        }

        if (!injected) {
          try {
            injectConfigs(field.get(obj), configName + ".", context);
          } catch (IllegalArgumentException | IllegalAccessException ex) {
            context.createIssue(CreationError.CREATION_003, ex.toString());
          }
        }
      }
    }
  }

  private void injectDefaultValue(Object obj, Field field, ConfigDefinition configDef, Context context) {
    Object defaultValue = configDef.getDefaultValue();
    if (defaultValue != null) {
      injectConfigValue(obj, field, defaultValue, configDef, context);
    } else if (!hasJavaDefault(obj, field)) {
      defaultValue = configDef.getType().getDefault(field.getType());
      injectConfigValue(obj, field, defaultValue, configDef, context);
    }
  }

  private boolean hasJavaDefault(Object obj, Field field) {
    try {
      return field.get(obj) != null;
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("Unexpected exception: {}", ex.toString()), ex);
    }
  }

  @SuppressWarnings("unchecked")
  Object toEnum(Class klass, Object value, String groupName, String configName, Context context) {
    try {
      value = Enum.valueOf(klass, value.toString());
    } catch (IllegalArgumentException ex) {
      context.createIssue(groupName, configName, CreationError.CREATION_010, value, klass.getSimpleName(), ex.toString());
      value = null;
    }
    return value;
  }

  Object toString(Object value, String groupName, String configName, Context context) {
    if (!(value instanceof String)) {
      context.createIssue(groupName, configName, CreationError.CREATION_011, value, value.getClass().getSimpleName());
      value = null;
    }
    return value;
  }

  Object toChar(Object value, String groupName, String configName, Context context) {
    if (value instanceof String) {
      String strValue = value.toString();
      if (!strValue.isEmpty() && strValue.startsWith("\\u") && strValue.length() > 5 &&
          strValue.substring(2).matches("^[0-9a-fA-F]+$")) {
        // To support non printable unicode control characters
        value = (char) Integer.parseInt(strValue.substring(2), 16 );
      } else if (strValue.isEmpty() || strValue.length() > 1) {
        context.createIssue(groupName, configName, CreationError.CREATION_012, value, strValue);
        value = null;
      } else {
        value = strValue.charAt(0);
      }
    } else if (!(value instanceof Character)) {
      String valueType = value == null ? "null" : value.getClass().getName();
      context.createIssue(groupName, configName, CreationError.CREATION_012, value, valueType);
      value = null;
    }
    return value;
  }

  Object toBoolean(Object value, String groupName, String configName, Context context) {
    if (!(value instanceof Boolean)) {
      context.createIssue(groupName, configName, CreationError.CREATION_013, value, value.getClass().getName());
      value = null;
    }
    return value;
  }

  private static final Map<Class<?>, Class<?>> PRIMITIVE_WRAPPER_MAP
      = new ImmutableMap.Builder<Class<?>, Class<?>>()
      .put(byte.class, Byte.class)
      .put(short.class, Short.class)
      .put(int.class, Integer.class)
      .put(long.class, Long.class)
      .put(float.class, Float.class)
      .put(double.class, Double.class)
      .build();

  private static final Map<Class<?>, Method> WRAPPERS_VALUE_OF_MAP = new HashMap<>();

  @SuppressWarnings("unchecked")
  private static Method getValueOfMethod(Class klass) {
    try {
      return klass.getMethod("valueOf", String.class);
    } catch (Exception ex)  {
      throw new RuntimeException(ex);
    }
  }

  static {
    for (Class klass : PRIMITIVE_WRAPPER_MAP.values()) {
      WRAPPERS_VALUE_OF_MAP.put(klass, getValueOfMethod(klass));
    }
  }

  Object toNumber(Class numberType, Object value, String groupName, String configName, Context context) {
    if (!ConfigValueExtractor.NUMBER_TYPES.contains(value.getClass())) {
      context.createIssue(groupName, configName, CreationError.CREATION_014, value, value.getClass());
      value = null;
    } else {
      try {
        if (PRIMITIVE_WRAPPER_MAP.containsKey(numberType)) {
          numberType = PRIMITIVE_WRAPPER_MAP.get(numberType);
        }
        value = WRAPPERS_VALUE_OF_MAP.get(numberType).invoke(null, value.toString());
      } catch (Exception ex) {
        context.createIssue(groupName, configName, CreationError.CREATION_015, value, numberType.getSimpleName(), ex.toString());
        value = null;
      }
    }
    return value;
  }

  Object toList(Object value, ConfigDefinition configDef, String groupName, String configName, Context context, Field field) {
    if (!(value instanceof List)) {
      context.createIssue(groupName, configName, CreationError.CREATION_020);
      value = null;
    } else {
      boolean error = false;
      List<Object> list = new ArrayList<>();
      for (Object element : (List) value) {
        if (element == null) {
          context.createIssue(groupName, configName,  CreationError.CREATION_021);
          error = true;
        } else {
          element = resolveIfImplicitEL(element, configDef, context);
          if (element != null) {
            //We support list of String and enums.
            //If the field type is enum and the element is String, convert to enum
            if(field != null) {
              Type type = field.getGenericType();
              if (type instanceof ParameterizedType) {
                Type type1 = ((ParameterizedType) type).getActualTypeArguments()[0];
                if(type1 instanceof Class && ((Class<?>)type1).isEnum()) {
                  element = toEnum((Class<?>)type1, element, groupName, configName, context);
                }
              }
            }
            list.add(element);
          } else {
            error = true;
          }
        }
      }
      value = (error) ? null : list;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  Object toMap(Object value, ConfigDefinition configDef, String groupName, String configName, Context context) {
    if (!(value instanceof List)) {
      // This should be a list of maps because in JSON we represent it as
      // [{"key": "actual key name", "value": "your value"}]
      context.createIssue(groupName, configName, CreationError.CREATION_030);
      value = null;
    } else {
      boolean error = false;
      Map map = new LinkedHashMap();
      for (Object entry : (List) value) {
        if (!(entry instanceof Map)) {
          error = true;
          context.createIssue(groupName, configName, CreationError.CREATION_031, entry.getClass().getSimpleName());
        } else {

          Object k = ((Map)entry).get("key");
          if (k == null) {
            context.createIssue(groupName, configName, CreationError.CREATION_032);
          }

          Object v = ((Map)entry).get("value");
          if (v == null) {
            context.createIssue(groupName, configName, CreationError.CREATION_033);
          } else {
            v = resolveIfImplicitEL(v, configDef, context);
          }

          if (k != null && v != null) {
            map.put(k, v);
          } else {
            error = true;
          }
        }
      }
      value = (error) ? null : map;
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  private Object toComplexField(Object value, ConfigDefinition configDef, Context context) {
    if (!(value instanceof List)) {
      context.createIssue(configDef.getGroup(), configDef.getName(), CreationError.CREATION_040, value.getClass().getSimpleName());
      value = null;
    } else {
      boolean error = false;
      List<Object> list = new ArrayList<>();
      String className = configDef.getModel().getListBeanClass().getName();
      try {
        // we need to use the classloader fo the stage to instatiate the ComplexField so if the stage has a private
        // classloader we use the same one.
        Class klass = Thread.currentThread().getContextClassLoader().loadClass(className);
        List listValue = (List) value;
        for (int i = 0; i < listValue.size(); i++) {
          Map<String, Object> configElement;
          try {
            configElement = (Map<String, Object>) listValue.get(i);
            try {
              Object element = klass.newInstance();
              if (createConfigBeans(element, configDef.getName() + ".", context)) {
                Context childContext = new OverrideContext(context, configElement, configDef.getModel().getConfigDefinitionsAsMap());
                injectConfigs(element, "", childContext);
                list.add(element);
              }
            } catch (InstantiationException | IllegalAccessException ex) {
              context.createIssue(configDef.getGroup(), Utils.format("{}[{}]", configDef.getName(), i), CreationError.CREATION_041, klass.getSimpleName(), ex.toString());
              error = true;
              break;
            }
          } catch (ClassCastException ex) {
            context.createIssue(configDef.getGroup(), Utils.format("{}[{}]", configDef.getName(), i), CreationError.CREATION_042, ex.toString());
          }
        }
        value = (error) ? null : list;
      } catch (ClassNotFoundException ex) {
        value = null;
        LOG.debug("Can't load class {}", className, ex);
        context.createIssue(
          configDef.getGroup(),
          configDef.getName(),
          CreationError.CREATION_043,
          ex.toString(),
          Thread.currentThread().getContextClassLoader().toString()
        );
      }
    }
    return value;
  }

  Object toCredentialValue(Object value, String groupName, String configName, Context context) {
    if (value instanceof String) {
      value = new ClearCredentialValue((String) value);
    } else if (!(value instanceof CredentialValue)) {
      context.createIssue(groupName, configName, CreationError.CREATION_012, value.getClass().getSimpleName());
    }
    return value;
  }

  private Object resolveIfImplicitEL(Object value, ConfigDefinition configDef, Context context) {
    if (configDef.getEvaluation() == ConfigDef.Evaluation.IMPLICIT && value instanceof String &&
        ElUtil.isElString(value)) {
      try {
        value = ElUtil.evaluate(context.errorDescription(), value, configDef, context.getPipelineConstants());
      } catch (Exception ex) {
        LOG.error(CreationError.CREATION_005.getMessage(), value, ex.toString(), ex);
        context.createIssue(configDef.getGroup(), configDef.getName(), CreationError.CREATION_005, value, ex.toString());
        value = null;
      }
    }
    return value;
  }

  public void injectConfigValue(Object obj, Field field, Object value, ConfigDefinition configDef, Context context) {
    String groupName = configDef.getGroup();
    String configName = configDef.getName();
    if (value == null) {
      context.createIssue(groupName, configName, CreationError.CREATION_050);
    } else {
      if (configDef.getModel() != null && configDef.getModel().getModelType() == ModelType.LIST_BEAN) {
        value = toComplexField(value, configDef, context);
      } else if (List.class.isAssignableFrom(field.getType())) {
        value = toList(value, configDef, groupName, configName, context, field);
      } else if (Map.class.isAssignableFrom(field.getType())) {
        value = toMap(value, configDef, groupName, configName, context);
      } else {
        value = resolveIfImplicitEL(value, configDef, context);
        if (value != null) {
          if (field.getType().isEnum()) {
            value = toEnum(field.getType(), value, groupName, configName, context);
          } else if (field.getType() == String.class) {
            value = toString(value, groupName, configName, context);
          } else if (List.class.isAssignableFrom(field.getType())) {
            value = toList(value, configDef, groupName, configName, context, field);
          } else if (Map.class.isAssignableFrom(field.getType())) {
            value = toMap(value, configDef, groupName, configName, context);
          } else if (ConfigValueExtractor.CHARACTER_TYPES.contains(field.getType())) {
            value = toChar(value, groupName, configName, context);
          } else if (ConfigValueExtractor.BOOLEAN_TYPES.contains(field.getType())) {
            value = toBoolean(value, groupName, configName, context);
          } else if (ConfigValueExtractor.NUMBER_TYPES.contains(field.getType())) {
            value = toNumber(field.getType(), value, groupName, configName, context);
          } else if (ConfigValueExtractor.isCredentialValueConfig(field.getType())) {
            value = toCredentialValue(value, groupName, configName, context);
          } else {
            context.createIssue(groupName, configName, CreationError.CREATION_051, field.getType().getSimpleName());
            value = null;
          }
        }
      }
      if (value != null) {
        try {
          field.set(obj, value);
        } catch (IllegalAccessException ex) {
          context.createIssue(groupName, configName, CreationError.CREATION_060, value, ex.toString());
        }
      }
    }
  }

  String getConnectionId(Object obj, Field connectionField, Context context) {
    String connectionId = null;
    Dependency[] dependencies = connectionField.getAnnotation(ConfigDefBean.class).dependencies();
    if (dependencies.length == 0) {
      context.createIssue(CreationError.CREATION_1106, "Connection does not have a dependent selection field");
    } else {
      String connectionSelectionFieldName = dependencies[0].configName();
      try {
        Field connectionSelectionField = obj.getClass().getField(connectionSelectionFieldName);
        connectionId = (String) connectionSelectionField.get(obj);
      } catch (Exception ex) {
        context.createIssue(CreationError.CREATION_1106, "Encountered error when trying to extract ID: " + ex.toString());
      }
    }
    return connectionId;
  }
}
