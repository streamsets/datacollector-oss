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
package com.streamsets.datacollector.stagelibrary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.classpath.ClasspathValidator;
import com.streamsets.datacollector.classpath.ClasspathValidatorResult;
import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.ErrorHandlingChooserValues;
import com.streamsets.datacollector.config.LineagePublisherDefinition;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.PipelineLifecycleStageChooserValues;
import com.streamsets.datacollector.config.PipelineRulesDefinition;
import com.streamsets.datacollector.config.PrivateClassLoaderDefinition;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.config.StatsTargetChooserValues;
import com.streamsets.datacollector.definition.CredentialStoreDefinitionExtractor;
import com.streamsets.datacollector.definition.LineagePublisherDefinitionExtractor;
import com.streamsets.datacollector.definition.ServiceDefinitionExtractor;
import com.streamsets.datacollector.definition.StageDefinitionExtractor;
import com.streamsets.datacollector.definition.StageLibraryDefinitionExtractor;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.json.JsonMapperImpl;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.ServiceRuntime;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.SDCClassLoader;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import com.streamsets.pipeline.api.impl.LocaleInContext;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ClassLoaderStageLibraryTask extends AbstractTask implements StageLibraryTask {
  public static final String MAX_PRIVATE_STAGE_CLASS_LOADERS_KEY = "max.stage.private.classloaders";
  public static final int MAX_PRIVATE_STAGE_CLASS_LOADERS_DEFAULT = 50;

  public static final String IGNORE_STAGE_DEFINITIONS = "ignore.stage.definitions";
  public static final String JAVA_UNSUPPORTED_REGEXP = "java.unsupported.regexp";

  private static final String CONFIG_LIBRARY_ALIAS_PREFIX = "library.alias.";
  private static final String CONFIG_STAGE_ALIAS_PREFIX = "stage.alias.";

  private static final String PROPERTIES_CP_WHITELIST = "data-collector-classpath-whitelist.properties";

  private static final String CONFIG_CP_VALIDATION = "stagelibs.classpath.validation.enable";
  private static final boolean DEFAULT_CP_VALIDATION = true;

  private static final String CONFIG_CP_VALIDATION_RESULT = "stagelibs.classpath.validation.terminate";
  private static final boolean DEFAULT_CP_VALIDATION_RESULT = false;

  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderStageLibraryTask.class);

  private final RuntimeInfo runtimeInfo;
  private final Map<String,String> libraryNameAliases;
  private final Map<String,String> stageNameAliases;
  private final Configuration configuration;
  private List<? extends ClassLoader> stageClassLoaders;
  private Map<String, StageDefinition> stageMap;
  private List<StageDefinition> stageList;
  private List<LineagePublisherDefinition> lineagePublisherDefinitions;
  private Map<String, LineagePublisherDefinition> lineagePublisherDefinitionMap;
  private List<CredentialStoreDefinition> credentialStoreDefinitions;
  private LoadingCache<Locale, List<StageDefinition>> localizedStageList;
  private List<ServiceDefinition> serviceList;
  private Map<Class, ServiceDefinition> serviceMap;
  private ObjectMapper json;
  private KeyedObjectPool<String, ClassLoader> privateClassLoaderPool;

  @Inject
  public ClassLoaderStageLibraryTask(RuntimeInfo runtimeInfo, Configuration configuration) {
    super("stageLibrary");
    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    Map<String, String> aliases = new HashMap<>();
    for (Map.Entry<String,String> entry
        : configuration.getSubSetConfiguration(CONFIG_LIBRARY_ALIAS_PREFIX).getValues().entrySet()) {
      aliases.put(entry.getKey().substring(CONFIG_LIBRARY_ALIAS_PREFIX.length()), entry.getValue());
    }
    libraryNameAliases = ImmutableMap.copyOf(aliases);
    aliases.clear();
    for (Map.Entry<String,String> entry
      : configuration.getSubSetConfiguration(CONFIG_STAGE_ALIAS_PREFIX).getValues().entrySet()) {
      aliases.put(entry.getKey().substring(CONFIG_STAGE_ALIAS_PREFIX.length()), entry.getValue());
    }
    stageNameAliases = ImmutableMap.copyOf(aliases);
  }

  private Method duplicateClassLoaderMethod;
  private Method getClassLoaderKeyMethod;
  private Method isPrivateClassLoaderMethod;

  private void resolveClassLoaderMethods(ClassLoader cl) {
    if (cl.getClass().getSimpleName().equals("SDCClassLoader")) {
      try {
        duplicateClassLoaderMethod = cl.getClass().getMethod("duplicateStageClassLoader");
        getClassLoaderKeyMethod = cl.getClass().getMethod("getName");
        isPrivateClassLoaderMethod = cl.getClass().getMethod("isPrivate");
      } catch (Exception ex) {
        throw new Error(ex);
      }
    } else {
      LOG.warn("No SDCClassLoaders available, there is no class isolation");
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T invoke(Method method, ClassLoader cl, Class<T> returnType) {
    try {
      return (T) method.invoke(cl);
    } catch (Exception ex) {
      throw new Error(ex);
    }
  }

  private ClassLoader duplicateClassLoader(ClassLoader cl) {
    return (duplicateClassLoaderMethod == null) ? cl : invoke(duplicateClassLoaderMethod, cl, ClassLoader.class);
  }

  private String getClassLoaderKey(ClassLoader cl) {
    return (getClassLoaderKeyMethod == null) ? "key" : invoke(getClassLoaderKeyMethod, cl, String.class);
  }

  private boolean isPrivateClassLoader(ClassLoader cl) {
    if (cl != getClass().getClassLoader()) { // if we are the container CL we are not private for sure
      return (isPrivateClassLoaderMethod == null) ? false : invoke(isPrivateClassLoaderMethod, cl, Boolean.class);
    } else {
      return  false;
    }
  }

  private class ClassLoaderFactory extends BaseKeyedPooledObjectFactory<String, ClassLoader> {
    private final Map<String, ClassLoader> classLoaderMap;

    public ClassLoaderFactory(List<? extends ClassLoader> classLoaders) {
      classLoaderMap = new HashMap<>();
      for (ClassLoader cl : classLoaders) {
        classLoaderMap.put(getClassLoaderKey(cl), cl);
      }
    }

    @Override
    public ClassLoader create(String key) throws Exception {
      return duplicateClassLoader(classLoaderMap.get(key));
    }

    @Override
    public PooledObject<ClassLoader> wrap(ClassLoader value) {
      return new DefaultPooledObject<>(value);
    }
  }

  @Override
  public void initTask() {
    super.initTask();
    stageClassLoaders = runtimeInfo.getStageLibraryClassLoaders();
    if (!stageClassLoaders.isEmpty()) {
      resolveClassLoaderMethods(stageClassLoaders.get(0));
    }

    if(configuration.get(CONFIG_CP_VALIDATION, DEFAULT_CP_VALIDATION)) {
      validateStageClasspaths();
    }

    // Load all stages and other objects from the libraries
    json = ObjectMapperFactory.get();
    stageList = new ArrayList<>();
    stageMap = new HashMap<>();
    lineagePublisherDefinitions = new ArrayList<>();
    lineagePublisherDefinitionMap = new HashMap<>();
    credentialStoreDefinitions = new ArrayList<>();
    serviceList = new ArrayList<>();
    serviceMap = new HashMap<>();
    loadStages();
    stageList = ImmutableList.copyOf(stageList);
    stageMap = ImmutableMap.copyOf(stageMap);
    lineagePublisherDefinitions = ImmutableList.copyOf(lineagePublisherDefinitions);
    lineagePublisherDefinitionMap = ImmutableMap.copyOf(lineagePublisherDefinitionMap);
    credentialStoreDefinitions = ImmutableList.copyOf(credentialStoreDefinitions);
    serviceList = ImmutableList.copyOf(serviceList);
    serviceMap = ImmutableMap.copyOf(serviceMap);

    // Various validations
    validateAllServicesAvailable();

    // localization cache for definitions
    localizedStageList = CacheBuilder.newBuilder().build(new CacheLoader<Locale, List<StageDefinition>>() {
      @Override
      public List<StageDefinition> load(Locale key) throws Exception {
        List<StageDefinition> list = new ArrayList<>();
        for (StageDefinition stage : stageList) {
          list.add(stage.localize());
        }
        return list;
      }
    });
    validateStageVersions(stageList);
    validateServices(stageList, serviceList);

    // initializing the list of targets that can be used for error handling
    ErrorHandlingChooserValues.setErrorHandlingOptions(this);

    // initializing the list of targets that can be used as aggregating sink
    StatsTargetChooserValues.setStatsTargetOptions(this);

    // initializing the list of targets that can be used for pipeline lifecycle events
    PipelineLifecycleStageChooserValues.setHandlingOptions(this);

    // initializing the pool of private stage classloaders
    GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
    poolConfig.setJmxEnabled(false);
    poolConfig.setMaxTotal(configuration.get(MAX_PRIVATE_STAGE_CLASS_LOADERS_KEY,
                                             MAX_PRIVATE_STAGE_CLASS_LOADERS_DEFAULT));
    poolConfig.setMinEvictableIdleTimeMillis(-1);
    poolConfig.setNumTestsPerEvictionRun(0);
    poolConfig.setMaxIdlePerKey(-1);
    poolConfig.setMinIdlePerKey(0);
    poolConfig.setMaxTotalPerKey(-1);
    poolConfig.setBlockWhenExhausted(false);
    poolConfig.setMaxWaitMillis(0);
    privateClassLoaderPool = new GenericKeyedObjectPool<>(new ClassLoaderFactory(stageClassLoaders), poolConfig);
  }

  private void validateStageClasspaths() {
    LOG.info("Validating classpath of all stages");

    // Firstly validate the stage classpaths for duplicate dependencies
    Set<String> corruptedClasspathStages = new HashSet<>();
    for(ClasspathValidatorResult result : validateStageLibClasspath()) {
      if (!result.isValid()) {
        result.logDetails();
        corruptedClasspathStages.add(result.getName());
      }
    }

    if (corruptedClasspathStages.isEmpty()) {
      LOG.info("Classpath of all stages passed validation");
    } else {
      LOG.error("The following stages have invalid classpath: {}", StringUtils.join(corruptedClasspathStages, ", "));

      boolean canTerminate = configuration.get(CONFIG_CP_VALIDATION_RESULT, DEFAULT_CP_VALIDATION_RESULT);
      if (canTerminate) {
        throw new RuntimeException("Invalid classpath detected for " + corruptedClasspathStages.size() + " stage libraries: " + StringUtils.join(corruptedClasspathStages, ", "));
      }
    }
  }

  /**
   * Validate service dependencies.
   *
   * Any error is considered fatal and RuntimeException() will be thrown that will terminate the SDC start up procedure.
   */
  private void validateAllServicesAvailable() {
    // Firstly validate that all stages have satisfied service dependencies
    List<String> missingServices = new LinkedList<>();

    for(StageDefinition stage : stageList) {
      for(ServiceDependencyDefinition service : stage.getServices()) {
        if(!serviceMap.containsKey(service.getService())) {
          missingServices.add(Utils.format("Stage {} is missing service {}", stage.getName(), service.getService().getName()));
        }
      }
    }
    if(!missingServices.isEmpty()) {
      throw new RuntimeException("Missing services: " + StringUtils.join(missingServices, ", "));
    }

    // Secondly ensure that all loaded services are compatible with what is supported by our runtime engine
    List<String> unsupportedServices = new LinkedList<>();
    for(ServiceDefinition serviceDefinition : serviceList) {
      if(!ServiceRuntime.supports(serviceDefinition.getProvides())) {
        unsupportedServices.add(serviceDefinition.getProvides().toString());
      }
    }
    if(!unsupportedServices.isEmpty()) {
      throw new RuntimeException("Unsupported services: " + StringUtils.join(unsupportedServices, ", "));
    }
  }

  @Override
  protected void stopTask() {
    privateClassLoaderPool.close();
    super.stopTask();
  }

  Properties loadClasspathWhitelist(ClassLoader cl) {
   try (InputStream is = cl.getResourceAsStream(PROPERTIES_CP_WHITELIST)) {
      if (is != null) {
        Properties props = new Properties();
        props.load(is);
        return props;
      }
   } catch (IOException e) {
     // Fail over to null, the resources simply doesn't exists or something - since it's optional file, it's fully
     // legal to get here.
   }

    return null;
  }

  String getPropertyFromLibraryProperties(ClassLoader cl, String property, String defaultValue) throws IOException {
   try (InputStream is = cl.getResourceAsStream(StageLibraryDefinitionExtractor.DATA_COLLECTOR_LIBRARY_PROPERTIES)) {
      if (is != null) {
        Properties props = new Properties();
        props.load(is);
        return props.getProperty(property, defaultValue);
      }
    }

    return null;
  }

  Set<String> loadIgnoreStagesList(StageLibraryDefinition libDef) throws IOException {
    Set<String> ignoreStages = new HashSet<>();

    String ignore = getPropertyFromLibraryProperties(libDef.getClassLoader(), IGNORE_STAGE_DEFINITIONS, "");
    if(!StringUtils.isEmpty(ignore)) {
      ignoreStages.addAll(Splitter.on(",").trimResults().splitToList(ignore));
    }

    return ignoreStages;
  }

  List<String> removeIgnoreStagesFromList(StageLibraryDefinition libDef, List<String> stages) throws IOException {
    List<String> list = new ArrayList<>();
    Set<String> ignoreStages = loadIgnoreStagesList(libDef);
    Iterator<String> iterator = stages.iterator();
    while (iterator.hasNext()) {
      String stage = iterator.next();
      if (ignoreStages.contains(stage)) {
        LOG.debug("Ignoring stage class '{}' from library '{}'", stage, libDef.getName());
      } else {
        list.add(stage);
      }
    }
    return list;
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  void loadStages() {
    String javaVersion = System.getProperty("java.version");

    if (LOG.isDebugEnabled()) {
      for (ClassLoader cl : stageClassLoaders) {
        LOG.debug("Found stage library '{}'", StageLibraryUtils.getLibraryName(cl));
      }
    }

    try {
      RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
      DataCollectorServices.instance().put(JsonMapper.SERVICE_KEY, new JsonMapperImpl());
    } catch (IOException e) {
      throw new RuntimeException(
        Utils.format("Could not load runtime configuration, '{}'", e.toString()), e);
    }

    try {
      int libs = 0;
      int stages = 0;
      int lineagePublishers = 0;
      int credentialStores = 0;
      int services = 0;
      long start = System.currentTimeMillis();
      LocaleInContext.set(Locale.getDefault());
      for (ClassLoader cl : stageClassLoaders) {
        try {
          // Before loading any stages, let's verify that given stage library is compatible with our current JVM version
          String unsupportedJvmVersion = getPropertyFromLibraryProperties(cl, JAVA_UNSUPPORTED_REGEXP, null);
          if(!StringUtils.isEmpty(unsupportedJvmVersion)) {
            if(javaVersion.matches(unsupportedJvmVersion)) {
              LOG.warn("Can't load stages from {} since they are not compatible with current JVM version", StageLibraryUtils.getLibraryName(cl));
              continue;
            } else {
              LOG.debug("Stage lib {} passed java compatibility test for '{}'", StageLibraryUtils.getLibraryName(cl), unsupportedJvmVersion);
            }
          }

          // Load stages from the stage library
          StageLibraryDefinition libDef = StageLibraryDefinitionExtractor.get().extract(cl);
          LOG.debug("Loading stages and plugins from library '{}'", libDef.getName());
          libs++;

          // Load Stages
          for(Class klass : loadClassesFromResource(libDef, cl, STAGES_DEFINITION_RESOURCE)) {
            stages++;
            StageDefinition stage = StageDefinitionExtractor.get().extract(libDef, klass, Utils.formatL("Library='{}'", libDef.getName()));
            String key = createKey(libDef.getName(), stage.getName());
            LOG.debug("Loaded stage '{}'  version {}", key, stage.getVersion());
            stageList.add(stage);
            stageMap.put(key, stage);
          }

          // Load Lineage publishers
          for(Class klass : loadClassesFromResource(libDef, cl, LINEAGE_PUBLISHERS_DEFINITION_RESOURCE)) {
            lineagePublishers++;
            LineagePublisherDefinition lineage = LineagePublisherDefinitionExtractor.get().extract(libDef, klass);
            String key = createKey(libDef.getName(), lineage.getName());
            LOG.debug("Loaded lineage plugin '{}'", key);
            lineagePublisherDefinitions.add(lineage);
            lineagePublisherDefinitionMap.put(key, lineage);
          }

          // Load Credential stores
          for(Class klass : loadClassesFromResource(libDef, cl, CREDENTIAL_STORE_DEFINITION_RESOURCE)) {
            credentialStores++;
            CredentialStoreDefinition def = CredentialStoreDefinitionExtractor.get().extract(libDef, klass);
            String key = createKey(libDef.getName(), def.getName());
            LOG.debug("Loaded credential store '{}'", key);
            credentialStoreDefinitions.add(def);
          }

          // Load Services
          for(Class klass : loadClassesFromResource(libDef, cl, SERVICE_DEFINITION_RESOURCE)) {
            services++;
            ServiceDefinition def = ServiceDefinitionExtractor.get().extract(libDef, klass);
            LOG.debug("Loaded service for '{}'", def.getProvides().getCanonicalName());
            serviceList.add(def);
            serviceMap.put(def.getProvides(), def);
          }
        } catch (IOException | ClassNotFoundException ex) {
          throw new RuntimeException(
              Utils.format("Could not load stages definition from '{}', {}", cl, ex.toString()), ex);
        }
      }
      LOG.debug(
        "Loaded '{}' libraries with a total of '{}' stages, '{}' lineage publishers, '{}' services and '{}' credentialStores in '{}ms'",
        libs,
        stages,
        lineagePublishers,
        services,
        credentialStores,
        System.currentTimeMillis() - start
      );
    } finally {
      LocaleInContext.set(null);
    }
  }

  private <T> List<Class<? extends T>> loadClassesFromResource(
    StageLibraryDefinition libDef,
    ClassLoader cl,
    String resourceName
  ) throws IOException, ClassNotFoundException {
    Set<String> dedup = new HashSet<>();
    List<Class<? extends T>> list = new ArrayList<>();

    // Load all resource files with given name
    Enumeration<URL> resources = cl.getResources(resourceName);
    while (resources.hasMoreElements()) {
      URL url = resources.nextElement();
      try (InputStream is = url.openStream()) {
        List<String> plugins = json.readValue(is, List.class);
        plugins = removeIgnoreStagesFromList(libDef, plugins);
        for (String className : plugins) {
          if(dedup.contains(className)) {
            throw new IllegalStateException(Utils.format(
              "Library '{}' contains more than one definition for '{}'",
              libDef.getName(), className));
          }
          dedup.add(className);
          list.add((Class<? extends T>) cl.loadClass(className));
        }
      }
    }

    return list;
  }

  void validateStageVersions(List<StageDefinition> stageList) {
    boolean err = false;
    Map<String, Set<Integer>> stageVersions = new HashMap<>();
    for (StageDefinition stage : stageList) {
      Set<Integer> versions = stageVersions.get(stage.getName());
      if (versions == null) {
        versions = new HashSet<>();
        stageVersions.put(stage.getName(), versions);
      }
      versions.add(stage.getVersion());
      err |= versions.size() > 1;
    }
    if (err) {
      List<String> errors = new ArrayList<>();
      for (Map.Entry<String, Set<Integer>> entry : stageVersions.entrySet()) {
        if (entry.getValue().size() > 1) {
          for (StageDefinition stage : stageList) {
            if (stage.getName().equals(entry.getKey())) {
              errors.add(Utils.format("Stage='{}' Version='{}' Library='{}'", stage.getName(), stage.getVersion(),
                stage.getLibrary()));
            }
          }
        }
      }
      LOG.error("There cannot be 2 different versions of the same stage: {}", errors);
      throw new RuntimeException(Utils.format("There cannot be 2 different versions of the same stage: {}", errors));
    }
  }

  void validateServices(List<StageDefinition> stages, List<ServiceDefinition> services) {
    List<String> errors = new ArrayList<>();

    // Firstly validate that there are no duplicate providers for each service
    Set<Class> duplicates = new HashSet<>();
    for(ServiceDefinition def: services) {
      if(duplicates.contains(def.getProvides())) {
        errors.add(Utils.format("Service {} have multiple implementations.", def.getProvides()));
      }

      duplicates.add(def.getProvides());
    }

    // TBD: Validate that we have services for all stage dependencies

    if(!errors.isEmpty()) {
      throw new RuntimeException(Utils.format("Validate errors when loading services: {}", errors));
    }
  }

  private String createKey(String library, String name) {
    return library + ":" + name;
  }

  @Override
  public PipelineDefinition getPipeline() {
    return PipelineDefinition.getPipelineDef();
  }

  @Override
  public PipelineRulesDefinition getPipelineRules() {
    return PipelineRulesDefinition.getPipelineRulesDef();
  }

  @Override
  public List<StageDefinition> getStages() {
    try {
      return (LocaleInContext.get() == null) ? stageList : localizedStageList.get(LocaleInContext.get());
    } catch (ExecutionException ex) {
      LOG.warn("Error loading locale '{}', {}", LocaleInContext.get(), ex.toString(), ex);
      return stageList;
    }
  }

  @Override
  public List<LineagePublisherDefinition> getLineagePublisherDefinitions() {
    return lineagePublisherDefinitions;
  }

  @Override
  public LineagePublisherDefinition getLineagePublisherDefinition(String library, String name) {
    return lineagePublisherDefinitionMap.get(createKey(library, name));
  }

  @Override
  public List<CredentialStoreDefinition> getCredentialStoreDefinitions() {
    return credentialStoreDefinitions;
  }

  @Override
  public List<ServiceDefinition> getServiceDefinitions() {
    return serviceList;
  }

  @Override
  public ServiceDefinition getServiceDefinition(Class serviceInterface, boolean forExecution) {
    ServiceDefinition serviceDefinition = serviceMap.get(serviceInterface);

    if(forExecution && serviceDefinition.isPrivateClassLoader()) {
      serviceDefinition = new ServiceDefinition(serviceDefinition, getStageClassLoader(serviceDefinition));
    }

    return serviceDefinition;
  }

  @Override
  @SuppressWarnings("unchecked")
  public StageDefinition getStage(String library, String name, boolean forExecution) {
    StageDefinition def = stageMap.get(createKey(library, name));
    if (forExecution &&  def.isPrivateClassLoader()) {
      def = new StageDefinition(def, getStageClassLoader(def));
    }
    return def;
  }

  @Override
  public Map<String, String> getLibraryNameAliases() {
    return libraryNameAliases;
  }

  @Override
  public Map<String, String> getStageNameAliases() {
    return stageNameAliases;
  }

  @Override
  public List<ClasspathValidatorResult> validateStageLibClasspath() {
    List<ClasspathValidatorResult> validators = new LinkedList<>();

    for (ClassLoader cl : stageClassLoaders) {
      if (cl instanceof SDCClassLoader) {
        SDCClassLoader sdcCl = (SDCClassLoader) cl;

        ClasspathValidatorResult validationResult = ClasspathValidator.newValidator(sdcCl.getName())
          .withURLs(sdcCl.getURLs())
          .validate(loadClasspathWhitelist(cl));

        validators.add(validationResult);
      }
    }

    return validators;
  }

  ClassLoader getStageClassLoader(PrivateClassLoaderDefinition stageDefinition) {
    ClassLoader cl = stageDefinition.getStageClassLoader();
    if (stageDefinition.isPrivateClassLoader()) {
      String key = getClassLoaderKey(cl);
      synchronized (privateClassLoaderPool) {
        try {
          cl = privateClassLoaderPool.borrowObject(key);
          LOG.debug("Got a private ClassLoader for '{}', for '{}', active private ClassLoaders='{}'",
              key, stageDefinition.getName(), privateClassLoaderPool.getNumActive());
        } catch (Exception ex) {
          String msg = Utils.format(
              "Could not get a private ClassLoader for '{}', for '{}', active private ClassLoaders='{}': {}",
              key, stageDefinition.getName(), privateClassLoaderPool.getNumActive(), ex.toString());
          LOG.warn(msg, ex);
          throw new RuntimeException(msg, ex);
        }
      }
    }
    return cl;
  }

  @Override
  public void releaseStageClassLoader(ClassLoader classLoader) {
    if (isPrivateClassLoader(classLoader)) {
      String key = getClassLoaderKey(classLoader);
      synchronized (privateClassLoaderPool){
        if (privateClassLoaderPool.getNumActive() > 0) {
          try {
            LOG.debug("Returning private ClassLoader for '{}'", key);
            privateClassLoaderPool.returnObject(key, classLoader);
            LOG.debug("Returned a private ClassLoader for '{}', active private ClassLoaders='{}'",
                key, privateClassLoaderPool.getNumActive());
          } catch (Exception ex) {
            LOG.warn("Could not return a private ClassLoader for '{}', active private ClassLoaders='{}'",
                key, privateClassLoaderPool.getNumActive());
            throw new RuntimeException(ex);
          }
        }
      }
    }
  }

}
