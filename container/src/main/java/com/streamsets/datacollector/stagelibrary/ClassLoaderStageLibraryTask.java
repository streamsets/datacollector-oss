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
import com.google.common.collect.Sets;
import com.streamsets.datacollector.classpath.ClasspathValidator;
import com.streamsets.datacollector.classpath.ClasspathValidatorResult;
import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.ErrorHandlingChooserValues;
import com.streamsets.datacollector.config.InterceptorDefinition;
import com.streamsets.datacollector.config.LineagePublisherDefinition;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.PipelineFragmentDefinition;
import com.streamsets.datacollector.config.PipelineLifecycleStageChooserValues;
import com.streamsets.datacollector.config.PipelineRulesDefinition;
import com.streamsets.datacollector.config.PipelineTestStageChooserValues;
import com.streamsets.datacollector.config.PrivateClassLoaderDefinition;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.config.StageLibraryDelegateDefinitition;
import com.streamsets.datacollector.config.StatsTargetChooserValues;
import com.streamsets.datacollector.definition.ConnectionDefinitionExtractor;
import com.streamsets.datacollector.definition.ConnectionVerifierDefinition;
import com.streamsets.datacollector.definition.ConnectionVerifierDefinitionExtractor;
import com.streamsets.datacollector.definition.CredentialStoreDefinitionExtractor;
import com.streamsets.datacollector.definition.EventDefinitionExtractor;
import com.streamsets.datacollector.definition.InterceptorDefinitionExtractor;
import com.streamsets.datacollector.definition.LineagePublisherDefinitionExtractor;
import com.streamsets.datacollector.definition.ServiceDefinitionExtractor;
import com.streamsets.datacollector.definition.StageDefinitionExtractor;
import com.streamsets.datacollector.definition.StageLibraryDefinitionExtractor;
import com.streamsets.datacollector.definition.StageLibraryDelegateDefinitionExtractor;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.json.JsonMapperImpl;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.SdcConfiguration;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.restapi.bean.EventDefinitionJson;
import com.streamsets.datacollector.restapi.bean.RepositoryManifestJson;
import com.streamsets.datacollector.restapi.bean.StageDefinitionMinimalJson;
import com.streamsets.datacollector.restapi.bean.StageInfoJson;
import com.streamsets.datacollector.restapi.bean.StageLibrariesJson;
import com.streamsets.datacollector.restapi.bean.StageLibraryManifestJson;
import com.streamsets.datacollector.runner.ServiceRuntime;
import com.streamsets.datacollector.runner.StageLibraryDelegateRuntime;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.Version;
import com.streamsets.pipeline.SDCClassLoader;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import com.streamsets.pipeline.api.impl.LocaleInContext;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ClassLoaderStageLibraryTask extends AbstractTask implements StageLibraryTask {
  public static final String MAX_PRIVATE_STAGE_CLASS_LOADERS_KEY = "max.stage.private.classloaders";
  public static final int MAX_PRIVATE_STAGE_CLASS_LOADERS_DEFAULT = 50;

  public static String getIgnoreStageDefinitions() {
    String propertyName = "ignore.stage.definitions";
    if (Boolean.getBoolean("streamsets.cloud")) {
      propertyName = "cloud." + propertyName;
    }
    return propertyName;
  }

  public static final String JAVA_UNSUPPORTED_REGEXP = "java.unsupported.regexp";
  public static final String MIN_SDC_VERSION = "min.sdc.version";

  private static final String CONFIG_LIBRARY_ALIAS_PREFIX = "library.alias.";
  private static final String CONFIG_STAGE_ALIAS_PREFIX = "stage.alias.";

  private static final String PROPERTIES_CP_WHITELIST = "data-collector-classpath-whitelist.properties";

  private static final String CONFIG_CP_VALIDATION = "stagelibs.classpath.validation.enable";
  private static final boolean DEFAULT_CP_VALIDATION = true;

  private static final String CONFIG_CP_VALIDATION_RESULT = "stagelibs.classpath.validation.terminate";
  private static final boolean DEFAULT_CP_VALIDATION_RESULT = false;

  public static final String CONFIG_LOAD_THREADS_MAX = "stagelibs.load.threads.max";
  public static final int DEFAULT_LOAD_THREADS_MAX = Runtime.getRuntime().availableProcessors();

  private static final String DEFAULT_REQUIRED_STAGELIBS = "";

  private static final String NIGHTLY_URL = "http://nightly.streamsets.com/datacollector/";
  private static final String ARCHIVES_URL = "http://archives.streamsets.com/datacollector/";
  private static final String LATEST = "latest";
  private static final String SNAPSHOT = "-SNAPSHOT";
  private static final String TARBALL_PATH = "/tarball/";
  private static final String LEGACY_TARBALL_PATH = "/legacy/";
  private static final String ENTERPRISE_PATH = "enterprise/";
  private static final String CONFIG_PACKAGE_MANAGER_REPOSITORY_LINKS = "package.manager.repository.links";
  private static final String REPOSITORY_MANIFEST_JSON_PATH = "repository.manifest.json";
  private static final String ADDITIONAL = "additional";
  private static final String PRIVATE_POOL_ACTIVE = "active";
  private static final String PRIVATE_POOL_IDLE = "idle";
  private static final String PRIVATE_POOL_MAX = "max";

  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderStageLibraryTask.class);

  private final RuntimeInfo runtimeInfo;
  private final BuildInfo buildInfo;
  private final Map<String,String> libraryNameAliases;
  private final Map<String,String> stageNameAliases;
  private final Configuration configuration;
  private List<? extends ClassLoader> stageClassLoaders;
  private List<StageLibraryDefinition> stageLibraries;
  private Map<String, StageLibraryDefinition> stageLibraryMap;
  private Map<String, StageDefinition> stageMap;
  private List<StageDefinition> stageList;
  private List<LineagePublisherDefinition> lineagePublisherDefinitions;
  private Map<String, LineagePublisherDefinition> lineagePublisherDefinitionMap;
  private List<CredentialStoreDefinition> credentialStoreDefinitions;
  private LoadingCache<Locale, List<StageDefinition>> localizedStageList;
  private List<ServiceDefinition> serviceList;
  private Map<Class, ServiceDefinition> serviceMap;
  private List<InterceptorDefinition> interceptorList;
  private List<StageLibraryDelegateDefinitition> delegateList;
  private Map<String, StageLibraryDelegateDefinitition> delegateMap;
  private Map<String, ConnectionDefinition> connectionMap;
  private Map<String, Set<ConnectionVerifierDefinition>> connectionVerifierMap;
  private ObjectMapper json;
  private KeyedObjectPool<String, ClassLoader> privateClassLoaderPool;
  private Map<String, Object> gaugeMap;
  private final Map<String, EventDefinitionJson> eventDefinitionMap = new HashMap<>();
  private volatile List<RepositoryManifestJson> repositoryManifestList = null;
  private List<StageDefinitionMinimalJson> stageDefinitionMinimalList;

  @Inject
  public ClassLoaderStageLibraryTask(RuntimeInfo runtimeInfo, BuildInfo buildInfo, Configuration configuration) {
    super("stageLibrary");
    this.runtimeInfo = runtimeInfo;
    this.buildInfo = buildInfo;
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

    String javaVersion = System.getProperty("java.version");
    Version sdcVersion = new Version(buildInfo.getVersion());

    // Initialize internal structures that keep records of various entities
    json = ObjectMapperFactory.get();
    stageLibraries = new ArrayList<>();
    stageLibraryMap = new HashMap<>();
    stageList = new ArrayList<>();
    stageMap = new HashMap<>();
    lineagePublisherDefinitions = new ArrayList<>();
    lineagePublisherDefinitionMap = new HashMap<>();
    credentialStoreDefinitions = new ArrayList<>();
    serviceList = new ArrayList<>();
    serviceMap = new HashMap<>();
    interceptorList = new ArrayList<>();
    delegateList = new ArrayList<>();
    delegateMap = new HashMap<>();
    connectionMap = new HashMap<>();
    connectionVerifierMap = new HashMap<>();

    // Initialize static classes
    try {
      RuntimeEL.loadRuntimeConfiguration(runtimeInfo);
      DataCollectorServices.instance().put(JsonMapper.SERVICE_KEY, new JsonMapperImpl());
    } catch (IOException e) {
      throw new RuntimeException(Utils.format("Could not load runtime configuration, '{}'", e.toString()), e);
    }

    // Finally load stage libraries, in parallel manner
    try {
      long start = System.currentTimeMillis();
      int maxThreads = configuration.get(CONFIG_LOAD_THREADS_MAX, DEFAULT_LOAD_THREADS_MAX);
      AtomicBoolean failure = new AtomicBoolean(false);

      ArrayBlockingQueue<ClassLoader> queue = new ArrayBlockingQueue<>(Math.max(stageClassLoaders.size(), 1));
      queue.addAll(stageClassLoaders);

      ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
      for(int i = 0; i < maxThreads; i++) {
        executor.execute(() -> {
          LocaleInContext.set(Locale.getDefault());
          ClassLoader cl;
          while ((cl = queue.poll()) != null) {
            try {
              loadStageLibrary(cl, javaVersion, sdcVersion);
            } catch (Throwable e) {
              LOG.error("Error while loading stage library", e);
              failure.set(true);
            }
          }
        });
      }

      executor.shutdown();
      if(!executor.awaitTermination(20, TimeUnit.MINUTES)) {
        throw new RuntimeException("Did not load all stage libraries in 20 minutes.");
      }

      if(failure.get()) {
        throw new RuntimeException("At least one of the stage libraries failed to load.");
      }

      LOG.info("Loaded {} libraries with a total of {} stages, {} lineage publishers, {} services, {} interceptors, " +
              "{} delegates, {} credentialStores, {} connections, and {} connection verifiers in {}",
          stageLibraries.size(),
          stageList.size(),
          lineagePublisherDefinitions.size(),
          serviceList.size(),
          interceptorList.size(),
          delegateList.size(),
          credentialStoreDefinitions.size(),
          connectionMap.size(),
          connectionVerifierMap.size(),
          DurationFormatUtils.formatDuration(System.currentTimeMillis() - start, "H:m:s.S", false)
      );
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed loading stage libraries", e);
    } finally {
      LocaleInContext.set(null);
    }

    // Ensure that internal structures won't change over time
    stageLibraries = ImmutableList.copyOf(stageLibraries);
    stageLibraryMap = ImmutableMap.copyOf(stageLibraryMap);
    stageList = ImmutableList.copyOf(stageList);
    stageMap = ImmutableMap.copyOf(stageMap);
    lineagePublisherDefinitions = ImmutableList.copyOf(lineagePublisherDefinitions);
    lineagePublisherDefinitionMap = ImmutableMap.copyOf(lineagePublisherDefinitionMap);
    credentialStoreDefinitions = ImmutableList.copyOf(credentialStoreDefinitions);
    serviceList = ImmutableList.copyOf(serviceList);
    serviceMap = ImmutableMap.copyOf(serviceMap);
    interceptorList = ImmutableList.copyOf(interceptorList);
    delegateList = ImmutableList.copyOf(delegateList);
    delegateMap = ImmutableMap.copyOf(delegateMap);
    connectionMap = ImmutableMap.copyOf(connectionMap);
    connectionVerifierMap = ImmutableMap.copyOf(connectionVerifierMap);

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
    validateAllServicesAvailable();
    validateStageVersions(stageList);
    validateServices(stageList, serviceList);
    validateDelegates(delegateList);
    validateRequiredStageLibraries();

    // initializing the list of targets that can be used for error handling
    ErrorHandlingChooserValues.setErrorHandlingOptions(this);

    // initializing the list of targets that can be used as aggregating sink
    StatsTargetChooserValues.setStatsTargetOptions(this);

    // initializing the list of targets that can be used for pipeline lifecycle events
    PipelineLifecycleStageChooserValues.setHandlingOptions(this);

    // initializing the list of sources that can be used for test stages
    PipelineTestStageChooserValues.setHandlingOptions(this);

    // initializing the pool of private stage classloaders
    GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
    poolConfig.setJmxEnabled(false);
    int maxPrivateClassloaders = configuration.get(MAX_PRIVATE_STAGE_CLASS_LOADERS_KEY, MAX_PRIVATE_STAGE_CLASS_LOADERS_DEFAULT);
    poolConfig.setMaxTotal(maxPrivateClassloaders);
    poolConfig.setMinEvictableIdleTimeMillis(-1);
    poolConfig.setNumTestsPerEvictionRun(0);
    poolConfig.setMaxIdlePerKey(-1);
    poolConfig.setMinIdlePerKey(0);
    poolConfig.setMaxTotalPerKey(-1);
    poolConfig.setBlockWhenExhausted(false);
    poolConfig.setMaxWaitMillis(0);
    privateClassLoaderPool = new GenericKeyedObjectPool<>(new ClassLoaderFactory(stageClassLoaders), poolConfig);

    // Monitoring of use of the private class loaders
    this.gaugeMap = MetricsConfigurator.createFrameworkGauge(
      runtimeInfo.getMetrics(),
      "classloader.private",
      "runtime",
      null
    ).getValue();
    this.gaugeMap.put(PRIVATE_POOL_ACTIVE, new AtomicInteger(0));
    this.gaugeMap.put(PRIVATE_POOL_IDLE, new AtomicInteger(0));
    this.gaugeMap.put(PRIVATE_POOL_MAX, maxPrivateClassloaders);

    if (!Boolean.getBoolean("streamsets.cloud")) {
      // auto load stage library definitions
      Thread thread = new Thread(this::getRepositoryManifestList);
      thread.setDaemon(true);
      thread.setName("ManifestFetcher");
      thread.start();
    }
  }

  /**
   * Validate that all required libraries are available and loaded.
   */
  @VisibleForTesting void validateRequiredStageLibraries() {
    // Name of all installed libraries
    Set<String> installedLibraries = stageLibraries.stream()
      .map(StageLibraryDefinition::getName)
      .collect(Collectors.toSet());

    // Required libraries
    Set<String> requiredLibraries = new HashSet<>();
    String config = configuration.get(SdcConfiguration.REQUIRED_STAGELIBS, DEFAULT_REQUIRED_STAGELIBS);
    for(String stageLib : config.split(",")) {
      if(!stageLib.isEmpty()) {
        requiredLibraries.add(stageLib);
      }
    }

    Set<String> missingLibraries = Sets.difference(requiredLibraries, installedLibraries);
    if(!missingLibraries.isEmpty()) {
      throw new RuntimeException(Utils.format(
        "Some required stage libraries are missing: {}",
        StringUtils.join(missingLibraries, ", ")
      ));
    }
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
        if(!serviceMap.containsKey(service.getServiceClass())) {
          missingServices.add(Utils.format("Stage {} is missing service {}", stage.getName(), service.getServiceClass().getName()));
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
    if (privateClassLoaderPool != null) {
      privateClassLoaderPool.close();
    }
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

    String ignore = getPropertyFromLibraryProperties(libDef.getClassLoader(), getIgnoreStageDefinitions(), "");
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

  private void loadStageLibrary(ClassLoader cl, String javaVersion, Version sdcVersion) {
    LOG.debug("Found stage library '{}'", StageLibraryUtils.getLibraryName(cl));

    // Local structures
    Map<String, StageDefinition> localStageMap = new HashMap<>();
    List<StageDefinition> localStageList = new LinkedList<>();
    List<LineagePublisherDefinition> localLineagePublisherDefinitions = new LinkedList<>();
    Map<String, LineagePublisherDefinition> localLineagePublisherDefinitionMap = new HashMap<>();
    List<CredentialStoreDefinition> localCredentialStoreDefinitions = new LinkedList<>();
    List<ServiceDefinition> localServiceList = new LinkedList<>();
    Map<Class, ServiceDefinition> localServiceMap= new HashMap<>();
    List<InterceptorDefinition> localInterceptorList = new LinkedList<>();
    List<StageLibraryDelegateDefinitition> localDelegateList = new LinkedList<>();
    Map<String, StageLibraryDelegateDefinitition> localDelegateMap = new HashMap<>();
    Map<String, EventDefinitionJson> localEventDefinitionMap = new HashMap<>();
    List<ConnectionDefinition> localConnectionList = new LinkedList<>();
    List<ConnectionVerifierDefinition> localConnectionVerifierList = new LinkedList<>();

    try {
      // Before loading any stages, let's verify that given stage library is compatible with our current JVM version
      String unsupportedJvmVersion = getPropertyFromLibraryProperties(cl, JAVA_UNSUPPORTED_REGEXP, null);
      if(!StringUtils.isEmpty(unsupportedJvmVersion)) {
        if(javaVersion.matches(unsupportedJvmVersion)) {
          LOG.warn("Can't load stages from {} since they are not compatible with current JVM version", StageLibraryUtils.getLibraryName(cl));
          return;
        } else {
          LOG.debug("Stage lib {} passed java compatibility test for '{}'", StageLibraryUtils.getLibraryName(cl), unsupportedJvmVersion);
        }
      }

      // And that this SDC is at least on requested version
      String minSdcVersion = getPropertyFromLibraryProperties(cl, MIN_SDC_VERSION, null);
      if(!StringUtils.isEmpty(minSdcVersion)) {
        if(!sdcVersion.isGreaterOrEqualTo(minSdcVersion)) {
          throw new IllegalArgumentException(
              Utils.format("Can't load stage library '{}' as it requires at least SDC version {} whereas current version is {}",
              StageLibraryUtils.getLibraryName(cl),
              minSdcVersion,
              buildInfo.getVersion()
            ));
        }
      }

      // Load stages from the stage library
      StageLibraryDefinition libDef = StageLibraryDefinitionExtractor.get().extract(cl);
      libDef.setVersion(getPropertyFromLibraryProperties(cl, "version", ""));
      LOG.debug("Loading stages and plugins from library '{}' on version {}", libDef.getName(), libDef.getVersion());
      synchronized (stageLibraries) {
        stageLibraries.add(libDef);
      }
      synchronized (stageLibraryMap) {
        stageLibraryMap.put(libDef.getName(), libDef);
      }

      // Load Stages
      for(Class klass : loadClassesFromResource(libDef, cl, STAGES_DEFINITION_RESOURCE)) {
        StageDefinition stage = StageDefinitionExtractor.get().extract(libDef, klass, Utils.formatL("Library='{}'", libDef.getName()));
        String key = createKey(libDef.getName(), stage.getName());
        LOG.debug("Loaded stage '{}'  version {}", key, stage.getVersion());
        localStageList.add(stage);
        localStageMap.put(key, stage);

        for(Class eventDefClass : stage.getEventDefs()) {
          if (!localEventDefinitionMap.containsKey(eventDefClass.getCanonicalName())) {
            localEventDefinitionMap.put(
                eventDefClass.getCanonicalName(),
                EventDefinitionExtractor.get().extractEventDefinition(eventDefClass)
            );
          }
        }
      }
      synchronized (stageMap) {
        stageMap.putAll(localStageMap);
      }
      synchronized (stageList) {
        stageList.addAll(localStageList);
      }
      synchronized (eventDefinitionMap) {
        eventDefinitionMap.putAll(localEventDefinitionMap);
      }

      // Load Lineage publishers
      for(Class klass : loadClassesFromResource(libDef, cl, LINEAGE_PUBLISHERS_DEFINITION_RESOURCE)) {
        LineagePublisherDefinition lineage = LineagePublisherDefinitionExtractor.get().extract(libDef, klass);
        String key = createKey(libDef.getName(), lineage.getName());
        LOG.debug("Loaded lineage plugin '{}'", key);
        localLineagePublisherDefinitions.add(lineage);
        localLineagePublisherDefinitionMap.put(key, lineage);
      }
      synchronized (lineagePublisherDefinitions) {
        lineagePublisherDefinitions.addAll(localLineagePublisherDefinitions);
      }
      synchronized (lineagePublisherDefinitionMap) {
        lineagePublisherDefinitionMap.putAll(localLineagePublisherDefinitionMap);
      }

      // Load Credential stores
      for(Class klass : loadClassesFromResource(libDef, cl, CREDENTIAL_STORE_DEFINITION_RESOURCE)) {
        CredentialStoreDefinition def = CredentialStoreDefinitionExtractor.get().extract(libDef, klass);
        String key = createKey(libDef.getName(), def.getName());
        LOG.debug("Loaded credential store '{}'", key);
        localCredentialStoreDefinitions.add(def);
      }
      synchronized (credentialStoreDefinitions) {
        credentialStoreDefinitions.addAll(localCredentialStoreDefinitions);
      }

      // Load Services
      for(Class klass : loadClassesFromResource(libDef, cl, SERVICE_DEFINITION_RESOURCE)) {
        ServiceDefinition def = ServiceDefinitionExtractor.get().extract(libDef, klass);
        LOG.debug("Loaded service for '{}'", def.getProvides().getCanonicalName());
        localServiceList.add(def);
        localServiceMap.put(def.getProvides(), def);
      }
      synchronized (serviceList) {
        serviceList.addAll(localServiceList);
      }
      synchronized (serviceMap) {
        serviceMap.putAll(localServiceMap);
      }

      // Load Interceptors
      for(Class klass : loadClassesFromResource(libDef, cl, INTERCEPTOR_DEFINITION_RESOURCE)) {
        InterceptorDefinition def = InterceptorDefinitionExtractor.get().extract(libDef, klass);
        LOG.debug("Loaded interceptor '{}'", def.getKlass().getCanonicalName());
        localInterceptorList.add(def);
      }
      synchronized (interceptorList) {
        interceptorList.addAll(localInterceptorList);
      }

      // Load Delegates
      for(Class klass : loadClassesFromResource(libDef, cl, DELEGATE_DEFINITION_RESOURCE)) {
        StageLibraryDelegateDefinitition def = StageLibraryDelegateDefinitionExtractor.get().extract(libDef, klass);
        String key = createKey(libDef.getName(), def.getExportedInterface().getCanonicalName());
        LOG.debug("Loaded delegate '{}'", def.getKlass().getCanonicalName());
        localDelegateList.add(def);
        localDelegateMap.put(key, def);
      }
      synchronized (delegateList) {
        delegateList.addAll(localDelegateList);
      }
      synchronized (delegateMap) {
        delegateMap.putAll(localDelegateMap);
      }

      // Load Connections
      for (Class klass : loadClassesFromResource(libDef, cl, CONNECTIONS_DEFINITION_RESOURCE)) {
        ConnectionDefinition def = ConnectionDefinitionExtractor.get().extract(libDef, klass);
        LOG.debug("Loaded connection '{}' from '{}'", def.getType(), libDef.getName());
        localConnectionList.add(def);
      }
      synchronized (connectionMap) {
        localConnectionList.forEach(connectionDefinition -> {
          ConnectionDefinition prevConnectionDefinition = connectionMap.get(connectionDefinition.getType());
          if (prevConnectionDefinition != null) {
            // We'll use the oldest version available when there's a conflict because it'll be the most compatible
            if (prevConnectionDefinition.getVersion() > connectionDefinition.getVersion()) {
              LOG.debug("Found connection version conflict for '{}' ({} vs {}), using {}",
                  connectionDefinition.getType(), connectionDefinition.getVersion(),
                  prevConnectionDefinition.getVersion(), prevConnectionDefinition.getVersion());
              connectionMap.put(connectionDefinition.getType(), connectionDefinition);
            }
          } else {
            connectionMap.put(connectionDefinition.getType(), connectionDefinition);
          }
        });

      }

      // Load Connection Verifiers
      for(Class klass : loadClassesFromResource(libDef, cl, CONNECTION_VERIFIERS_DEFINITION_RESOURCE)) {
        ConnectionVerifierDefinition def = ConnectionVerifierDefinitionExtractor.get().extract(libDef, klass);
        LOG.debug("Loaded connection verifier: '{}' from '{}'", def.getVerifierType(), libDef.getName());
        localConnectionVerifierList.add(def);
      }
      synchronized (connectionVerifierMap) {
        localConnectionVerifierList.forEach(connectionVerifierDefinition -> {
          Set<ConnectionVerifierDefinition> existingConnectionVerifierSet =
              connectionVerifierMap.computeIfAbsent(connectionVerifierDefinition.getVerifierType(),
                  k -> new HashSet<>());
          existingConnectionVerifierSet.add(connectionVerifierDefinition);
        });
      }

    } catch (IOException | ClassNotFoundException ex) {
      throw new RuntimeException(
          Utils.format("Could not load stages definition from '{}', {}", cl, ex.toString()), ex);
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

  @VisibleForTesting
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

  @VisibleForTesting
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

  @VisibleForTesting
  void validateDelegates(List<StageLibraryDelegateDefinitition> delegateList) {
    Set<String> errors = new HashSet<>();
    Map<String, Set<Class>> delegatesPerStageLib = new HashMap<>();

    // The current validation is only that each delegate interface can be exported max once from given stage library
    for(StageLibraryDelegateDefinitition def : delegateList) {
      String name = def.getLibraryDefinition().getName();

      Set<Class> declaredDelegates = delegatesPerStageLib.computeIfAbsent(name, (n) -> new HashSet<>());

      if(declaredDelegates.contains(def.getExportedInterface())) {
        errors.add(Utils.format(
          "Stage library '{}' exports delegate for '{}' more then once",
          name,
          def.getExportedInterface().getCanonicalName()
        ));
      }

      declaredDelegates.add(def.getExportedInterface());

      if(!StageLibraryDelegateRuntime.supports(def.getExportedInterface())) {
        errors.add(Utils.format(
          "Delegate interface {} is not supported by this runtime",
          def.getExportedInterface()
        ));
      }
    }

    if(!errors.isEmpty()) {
      throw new RuntimeException(Utils.format("Validate errors when loading delegates: {}", errors));
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
  public PipelineFragmentDefinition getPipelineFragment() {
    return PipelineFragmentDefinition.getPipelineFragmentDef();
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
  public List<InterceptorDefinition> getInterceptorDefinitions() {
    return interceptorList;
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
    long startTime = System.currentTimeMillis();
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

    LOG.info("Finished classpath validation in {} ms", System.currentTimeMillis() - startTime);
    return validators;
  }

  @Override
  public List<StageLibraryDelegateDefinitition> getStageLibraryDelegateDefinitions() {
    return delegateList;
  }

  @Override
  public StageLibraryDelegateDefinitition getStageLibraryDelegateDefinition(String stageLibrary, Class exportedInterface) {
    return delegateMap.get(createKey(stageLibrary, exportedInterface.getCanonicalName()));
  }

  @Override
  public List<StageLibraryDefinition> getLoadedStageLibraries() {
    return stageLibraries;
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
        } finally {
          updatePrivateClassLoaderPoolMetrics();
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
          } finally {
            updatePrivateClassLoaderPoolMetrics();
          }
        }
      }
    }
  }

  @Override
  public List<RepositoryManifestJson> getRepositoryManifestList() {
    if (repositoryManifestList == null &&  !Boolean.getBoolean("streamsets.cloud")) {
      Instant start = Instant.now();

      // initialize when it is called for first time
      repositoryManifestList = new ArrayList<>();

      // Initialize Repository Links
      String [] repoURLList;
      String repoLinksStr = configuration.get(CONFIG_PACKAGE_MANAGER_REPOSITORY_LINKS, "");
      if (StringUtils.isEmpty(repoLinksStr)) {
        String version = buildInfo.getVersion();
        String repoUrl = ARCHIVES_URL + version + TARBALL_PATH;
        String legacyRepoUrl = ARCHIVES_URL + version + LEGACY_TARBALL_PATH;
        if (version.contains(SNAPSHOT)) {
          repoUrl = NIGHTLY_URL + LATEST + TARBALL_PATH;
          legacyRepoUrl = NIGHTLY_URL + LATEST + LEGACY_TARBALL_PATH;
        }
        repoURLList = new String[] {
            repoUrl,
            repoUrl + ENTERPRISE_PATH,
            legacyRepoUrl
        };
      } else {
        repoURLList = repoLinksStr.split(",");
      }

      List<StageLibraryManifestJson> installedLibraries = new ArrayList<>();
      List<StageLibraryManifestJson> additionalLibraries = new ArrayList<>();

      Map<String, List<StageInfoJson>> installedStagesMap = new HashMap<>();
      for(StageDefinition stageDefinition: getStages()) {
        List<StageInfoJson> stagesList;
        if (installedStagesMap.containsKey(stageDefinition.getLibrary())) {
          stagesList = installedStagesMap.get(stageDefinition.getLibrary());
        } else {
          stagesList = new ArrayList<>();
          installedStagesMap.put(stageDefinition.getLibrary(), stagesList);
        }
        stagesList.add(new StageInfoJson(stageDefinition));
      }

      Map<String, Boolean> installedLibrariesMap = new HashMap<>();
      for(StageLibraryDefinition libDef : getLoadedStageLibraries()) {
        installedLibrariesMap.put(libDef.getName() + "::" + libDef.getVersion(), true);
        installedLibraries.add(new StageLibraryManifestJson(
            libDef.getName(),
            libDef.getLabel(),
            installedStagesMap.getOrDefault(libDef.getName(), Collections.emptyList()),
            true
        ));
      }

      Set<String> addedLibraryIds = new HashSet<>();

      for (String repoUrl: repoURLList) {
        if (!repoUrl.endsWith("/")) {
          repoUrl = repoUrl + "/";
        }
        String repoManifestUrl = repoUrl +  REPOSITORY_MANIFEST_JSON_PATH;
        LOG.info("Reading from Repository Manifest URL: " + repoManifestUrl);
        RepositoryManifestJson repositoryManifestJson = getRepositoryManifestFile(repoManifestUrl);
        if (repositoryManifestJson != null) {
          repositoryManifestJson.setRepoUrl(repoUrl);
          for(StageLibrariesJson stageLibrariesJson: repositoryManifestJson.getStageLibraries()) {
            String stageLibManifestUrl = repoUrl + stageLibrariesJson.getStagelibManifest();
            StageLibraryManifestJson stageLibraryManifestJson = getStageLibraryManifestJson(stageLibManifestUrl);
            if (stageLibraryManifestJson != null) {
              stageLibraryManifestJson.setInstalled(
                  installedLibrariesMap.containsKey(stageLibraryManifestJson.getStageLibId() + "::" + stageLibrariesJson.getStagelibVersion())
              );
              stageLibraryManifestJson.setStageLibFile(repoUrl + stageLibraryManifestJson.getStageLibFile());
              stageLibrariesJson.setStageLibraryManifest(stageLibraryManifestJson);
              if (repoUrl.contains(LEGACY_TARBALL_PATH)) {
                stageLibrariesJson.setLegacy(true);
              }
              addedLibraryIds.add(stageLibraryManifestJson.getStageLibId());
            }
          }
          repositoryManifestList.add(repositoryManifestJson);
        }
      }

      // Add installed custom/user stage libraries to the list which are not part of Archives manifest file
      List<StageLibrariesJson> additionalList = new ArrayList<>();
      for (StageLibraryManifestJson installedLibrary : installedLibraries) {
        if (!addedLibraryIds.contains(installedLibrary.getStageLibId())) {
          additionalLibraries.add(installedLibrary);
          StageLibrariesJson stageLibrariesJson = new StageLibrariesJson();
          stageLibrariesJson.setStageLibraryManifest(installedLibrary);
          additionalList.add(stageLibrariesJson);
        }
      }

      if (!additionalLibraries.isEmpty()) {
        RepositoryManifestJson additionalRepo = new RepositoryManifestJson();
        additionalRepo.setRepoUrl(ADDITIONAL);
        additionalRepo.setRepoLabel(ADDITIONAL);
        additionalRepo.setStageLibraries(additionalList);
        repositoryManifestList.add(additionalRepo);
      }

      Instant end = Instant.now();
      Duration timeElapsed = Duration.between(start, end);
      LOG.debug("Time taken for fetching repository manifest files : "+ timeElapsed.getSeconds() + " seconds");
    }

    return repositoryManifestList;
  }

  @Override
  public boolean isMultipleOriginSupported() {
    return false;
  }

  @Override
  public List<String> getLegacyStageLibs() {
    return ImmutableList.of(
      "streamsets-datacollector-apache-kafka_0_10-lib",
      "streamsets-datacollector-apache-kafka_0_8_1-lib",
      "streamsets-datacollector-apache-kafka_0_8_2-lib",
      "streamsets-datacollector-apache-kafka_0_9-lib",
      "streamsets-datacollector-apache-kafka_0_11-lib",
      "streamsets-datacollector-apache-kudu_1_0-lib",
      "streamsets-datacollector-apache-kudu_1_1-lib",
      "streamsets-datacollector-apache-kudu_1_2-lib",
      "streamsets-datacollector-cdh-spark_2_1-lib",
      "streamsets-datacollector-cdh_5_2-lib",
      "streamsets-datacollector-cdh_5_3-lib",
      "streamsets-datacollector-cdh_5_4-lib",
      "streamsets-datacollector-cdh_5_5-lib",
      "streamsets-datacollector-cdh_5_7-lib",
      "streamsets-datacollector-cdh_5_8-lib",
      "streamsets-datacollector-cdh_5_9-lib",
      "streamsets-datacollector-cdh_5_10-lib",
      "streamsets-datacollector-cdh_5_11-lib",
      "streamsets-datacollector-cdh_5_12-lib",
      "streamsets-datacollector-cdh_5_13-lib",
      "streamsets-datacollector-cdh_kafka_1_2-lib",
      "streamsets-datacollector-cdh_kafka_1_3-lib",
      "streamsets-datacollector-cdh_kafka_2_0-lib",
      "streamsets-datacollector-cdh_kafka_2_1-lib",
      "streamsets-datacollector-cdh_kafka_3_0-lib",
      "streamsets-datacollector-hdp_2_2-lib",
      "streamsets-datacollector-hdp_2_3-hive1-lib",
      "streamsets-datacollector-hdp_2_3-lib",
      "streamsets-datacollector-hdp_2_4-hive1-lib",
      "streamsets-datacollector-hdp_2_4-lib",
      "streamsets-datacollector-hdp_2_5-flume-lib",
      "streamsets-datacollector-hdp_2_5-lib",
      "streamsets-datacollector-hdp_2_6-lib",
      "streamsets-datacollector-hdp_2_6_1-hive1-lib",
      "streamsets-datacollector-hdp_2_6_2-hive1-lib",
      "streamsets-datacollector-hdp_2_6-hive2-lib",
      "streamsets-datacollector-hdp_2_6-flume-lib",
      "streamsets-datacollector-mapr_5_0-lib",
      "streamsets-datacollector-mapr_5_1-lib"
    );
  }

  @Override
  public Map<String, EventDefinitionJson> getEventDefinitions() {
    return eventDefinitionMap;
  }

  private RepositoryManifestJson getRepositoryManifestFile(String repoUrl) {
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.property(ClientProperties.READ_TIMEOUT, 2000);
    clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 2000);
    RepositoryManifestJson repositoryManifestJson = null;
    try (Response response = ClientBuilder.newClient(clientConfig).target(repoUrl).request().get()) {
      InputStream inputStream = response.readEntity(InputStream.class);
      repositoryManifestJson = ObjectMapperFactory.get().readValue(inputStream, RepositoryManifestJson.class);
    } catch (Exception ex) {
      LOG.error("Failed to read repository manifest json", ex);
    }
    return repositoryManifestJson;
  }

  private StageLibraryManifestJson getStageLibraryManifestJson(String stageLibManifestUrl) {
    StageLibraryManifestJson stageLibManifestJson = null;
    try (Response response = ClientBuilder.newClient().target(stageLibManifestUrl).request().get()) {
      InputStream inputStream = response.readEntity(InputStream.class);
      stageLibManifestJson = ObjectMapperFactory.get().readValue(inputStream, StageLibraryManifestJson.class);
    }  catch (Exception ex) {
      LOG.error("Failed to read stage-lib-manifest.json", ex);
    }
    return stageLibManifestJson;
  }

  private void updatePrivateClassLoaderPoolMetrics() {
    ((AtomicInteger)this.gaugeMap.get(PRIVATE_POOL_ACTIVE)).set(privateClassLoaderPool.getNumActive());
    ((AtomicInteger)this.gaugeMap.get(PRIVATE_POOL_IDLE)).set(privateClassLoaderPool.getNumIdle());
  }

  @Override
  public StageLibraryDefinition getStageLibraryDefinition(String libraryName) {
    return this.stageLibraryMap.get(libraryName);
  }

  @Override
  public Collection<ConnectionDefinition> getConnections() {
    return connectionMap.values();
  }

  @Override
  public ConnectionDefinition getConnection(String type) {
    return connectionMap.get(type);
  }

  @Override
  public Set<ConnectionVerifierDefinition> getConnectionVerifiers(String type) {
    return connectionVerifierMap.getOrDefault(type, Collections.emptySet());
  }

  @Override
  public synchronized List<StageDefinitionMinimalJson> getStageDefinitionMinimalList() {
    if (stageDefinitionMinimalList == null) {
      stageDefinitionMinimalList = new ArrayList<>();
      for (StageDefinition stageDefinition: getStages()) {
        stageDefinitionMinimalList.add(new StageDefinitionMinimalJson(
            stageDefinition.getName(),
            String.valueOf(stageDefinition.getVersion()),
            stageDefinition.getLibrary(),
            stageDefinition.getLibraryLabel()
        ));
      }
    }
    return stageDefinitionMinimalList;
  }
}
