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
package com.streamsets.datacollector.main;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.definition.ConcreteELDefinitionExtractor;
import com.streamsets.datacollector.execution.alerts.DataRuleEvaluator;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.DefinitionsJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineDefinitionJson;
import com.streamsets.datacollector.restapi.bean.PipelineFragmentDefinitionJson;
import com.streamsets.datacollector.restapi.bean.PipelineRulesDefinitionJson;
import com.streamsets.datacollector.restapi.bean.StageDefinitionJson;
import com.streamsets.datacollector.stagelibrary.ClassLoaderStageLibraryTask;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.PipelineCreator;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.BootstrapMain;
import com.streamsets.pipeline.SDCClassLoader;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MetadataGeneratorMain {

  private static final BuildInfo BUILD_INFO = ProductBuildInfo.getDefault();

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    System.setProperty("sdc.conf.dir", System.getProperty("user.dir"));
    Configuration.setFileRefsBaseDir(new File(System.getProperty("user.dir")));

    Cli.CliBuilder<Runnable> cliBuilder = Cli.
        <Runnable>builder("streamsets metadata-generator")
        .withDescription("Generates pipeline executor and stage libraries metadata for StreamSets Cloud")
        .withDefaultCommand(Help.class)
        .withCommands(
            Help.class,
            ExecutorMetadataCommand.class,
            StageLibMetadataCommand.class
        );

    Runnable runnable = cliBuilder.build().parse(args);
    runnable.run();
  }


  private static final String EL_CONSTANT_DEFS = "elConstantDefinitions";
  private static final String EL_FUNCTION_DEFS = "elFunctionDefinitions";


  public static abstract class MetadataCommand implements Runnable {

    @Option(
        name = {"-o", "--output-dir"},
        description = "Directory to write metadata to",
        arity = 1,
        required = true
    )
    protected String output;

    @NotNull
    protected SDCClassLoader getStagelibClassLoader(File stagelibJarsDir) throws MalformedURLException {
      File[] jars = stagelibJarsDir.listFiles(pathname -> pathname.getName().toLowerCase().endsWith(".jar"));
      List<URL> urls = new ArrayList<>();
      for (File jar : jars) {
        urls.add(jar.toURI().toURL());
      }
      return SDCClassLoader.getStageClassLoader(
          "type",
          stagelibJarsDir.getParentFile().getName(),
          urls,
          Thread.currentThread().getContextClassLoader(),
          false
      );
    }

    protected List<ClassLoader> getClassLoaders() throws Exception {
      String streamsetsLibs = System.getProperty("sdc.libraries.dir");
      if (streamsetsLibs == null) {
        System.err.println("ERROR: Missing 'sdc.libraries.dir' system properties");
        System.exit(1);
      }
      File dataFormatJarsLib = new File(streamsetsLibs, "/streamsets-datacollector-dataformats-lib/lib");
      SDCClassLoader dataFormatClassLoader = getStagelibClassLoader(dataFormatJarsLib);
      return ImmutableList.of(dataFormatClassLoader);
    }

    protected void extractIcon(StageDefinition stageDefinition) {
      String iconFile = stageDefinition.getIcon();
      if (iconFile != null && iconFile.trim().length() > 0) {
        if (iconFile.startsWith("/")) {
          iconFile = iconFile.substring(1);
        }
        File newIconFile = new File(new File(output), stageDefinition.getIcon());
        File newIconDir = newIconFile.getParentFile();
        if (!newIconDir.exists()) {
          newIconDir.mkdir();
        }
        try (
            InputStream iconIn = stageDefinition.getStageClassLoader().getResourceAsStream(iconFile);
            OutputStream iconOut = new FileOutputStream(newIconFile)
        ) {
          IOUtils.copy(iconIn, iconOut);
        } catch (IOException e) {
          System.err.printf("ERROR: Could not extract icon for '%s': %s", stageDefinition.getName(), e);
          System.exit(1);
        }
      }
    }

    protected void extractYamlUpgrader(StageDefinition stageDefinition) {
      String yamlFile = stageDefinition.getYamlUpgrader();
      if (yamlFile != null && yamlFile.trim().length() > 0) {
        if (yamlFile.startsWith("/")) {
          yamlFile = yamlFile.substring(1);
        }
        File newyamlFile = new File(new File(output), stageDefinition.getYamlUpgrader());
        File newyamlDir = newyamlFile.getParentFile();
        if (!newyamlDir.exists()) {
          newyamlDir.mkdir();
        }
        try (
            InputStream yamlIn = stageDefinition.getStageClassLoader().getResourceAsStream(yamlFile);
            OutputStream yamlOut = new FileOutputStream(newyamlFile)
        ) {
          IOUtils.copy(yamlIn, yamlOut);
        } catch (IOException e) {
          System.err.printf("ERROR: Could not extract yaml upgrader for '%s': %s", stageDefinition.getName(), e);
          System.exit(1);
        }
      }
    }


    protected abstract DefinitionsJson getMetadata(StageLibraryTask task);

    protected void generateAdditionalMetadata(RuntimeInfo runtimeInfo, StageLibraryTask task) throws IOException {

    }

    @Override
    public void run() {
      try {
        RuntimeInfo runtimeInfo = new RuntimeInfo(
            RuntimeInfo.SDC_PRODUCT,
            "sdc",
            new MetricRegistry(),
            getClassLoaders()
        ) {
          @Override
          public void init() {
          }

          @Override
          public String getId() {
            return null;
          }

          @Override
          public String getMasterSDCId() {
            return null;
          }

          @Override
          public String getRuntimeDir() {
            return System.getProperty("user.dir");
          }

          @Override
          public boolean isClusterSlave() {
            return false;
          }
        };

        ClassLoaderStageLibraryTask task = new ClassLoaderStageLibraryTask(
            runtimeInfo,
            BUILD_INFO,
            new Configuration()
        );
        task.init();
        task.run();
        File outputDir = new File(output);
        if (outputDir.exists()) {
          System.err.printf("ERROR: Directory '%s' already exists", outputDir.getAbsolutePath());
          System.exit(1);
        }
        if (!outputDir.mkdirs()) {
          System.err.printf("ERROR: Directory '%s' could not be created", outputDir.getAbsolutePath());
          System.exit(1);

        }
        try (PrintStream out = new PrintStream(new FileOutputStream(new File(output, "metadata.json")))) {
          // the getMetadata extracts icons and yaml upgraders to the output directory
          ObjectMapperFactory.get().writeValue(out, getMetadata(task));
        }
        generateAdditionalMetadata(runtimeInfo, task);
        task.stop();
      } catch (Exception ex) {
        System.err.printf("ERROR:Generating metadata: %s", ex);
        System.exit(1);
      }
    }

  }

  @Command(name = "pipeline-executor")
  public static class ExecutorMetadataCommand extends MetadataCommand {

    @Override
    protected DefinitionsJson getMetadata(StageLibraryTask stageLibrary) {
      // Populate the definitions with all the stage definitions
      List stageDefinitions = stageLibrary.getStages();

      DefinitionsJson definitions = new DefinitionsJson();

      definitions.setExecutorVersion(ProductBuildInfo.getDefault().getVersion());

      // Populate the definitions with the PipelineDefinition
      List<PipelineDefinitionJson> pipeline = new ArrayList<>(1);
      pipeline.add(BeanHelper.wrapPipelineDefinition(stageLibrary.getPipeline()));
      definitions.setPipeline(pipeline);

      // Populate the definitions with the PipelineFragmentDefinition
      List<PipelineFragmentDefinitionJson> pipelineFragment = new ArrayList<>(1);
      pipelineFragment.add(BeanHelper.wrapPipelineFragmentDefinition(stageLibrary.getPipelineFragment()));
      definitions.setPipelineFragment(pipelineFragment);

      // Populate service definitions
      List<ServiceDefinition> serviceDefinitions = stageLibrary.getServiceDefinitions();
      definitions.setServices(BeanHelper.wrapServiceDefinitions(serviceDefinitions));

      //Populate the definitions with the PipelineRulesDefinition
      List<PipelineRulesDefinitionJson> pipelineRules = new ArrayList<>(1);
      pipelineRules.add(BeanHelper.wrapPipelineRulesDefinition(stageLibrary.getPipelineRules()));
      definitions.setPipelineRules(pipelineRules);

      definitions.setRulesElMetadata(DataRuleEvaluator.getELDefinitions());

      Map<String, Object> map = new HashMap<>();
      map.put(
          EL_FUNCTION_DEFS,
          BeanHelper.wrapElFunctionDefinitionsIdx(ConcreteELDefinitionExtractor.get().getElFunctionsCatalog())
      );
      map.put(
          EL_CONSTANT_DEFS,
          BeanHelper.wrapElConstantDefinitionsIdx(ConcreteELDefinitionExtractor.get().getELConstantsCatalog())
      );
      definitions.setElCatalog(map);

      return definitions;
    }

    public ExecutorMetadataCommand() {
    }

    @Override
    protected void generateAdditionalMetadata(RuntimeInfo runtimeInfo, StageLibraryTask task) throws IOException {
      PipelineCreator pipelineCreator = new PipelineCreator(
          task.getPipeline(),
          PipelineStoreTask.SCHEMA_VERSION,
          BUILD_INFO.getVersion(),
          runtimeInfo.getId(),
          () -> null,
          () -> null,
          () -> null
      );

      PipelineConfiguration pipelineConfiguration = pipelineCreator.create(null, null, null, null, null);
      PipelineConfigurationJson pipelineConfigurationJson = new PipelineConfigurationJson(pipelineConfiguration);

      try (PrintStream out = new PrintStream(new FileOutputStream(new File(output, "empty-pipeline.json")))) {
        // the getMetadata extracts icons and yaml upgraders to the output directory
        ObjectMapperFactory.get().writeValue(out, pipelineConfigurationJson);
      }

    }

  }

  @Command(name = "stagelib")
  public static class StageLibMetadataCommand extends MetadataCommand {

    @Option(
        name = {"-s", "--stagelib-dir"},
        description = "Base directory of the stage library to generate metadata from",
        arity = 1,
        required = true
    )
    private String stagelibDir;

    protected File getStagelibDir() {
      File stagelibDirFile = new File(stagelibDir).getAbsoluteFile();
      if (!stagelibDirFile.exists()) {
        System.err.printf("ERROR: Stage library directory '%s\n'", stagelibDir);
        System.exit(1);
      }
      File stagelibLibDirFile = new File(stagelibDirFile, "lib");
      if (!stagelibLibDirFile.exists()) {
        System.err.printf("ERROR: Stage library lib/ directory '%s\n'", stagelibLibDirFile);
        System.exit(1);
      }
      return stagelibLibDirFile;
    }

    @Override
    protected List<ClassLoader> getClassLoaders() throws Exception {
      return ImmutableList.<ClassLoader>builder()
          .addAll(super.getClassLoaders())
          .add(getStagelibClassLoader(getStagelibDir()))
          .build();
    }

    @Override
    protected DefinitionsJson getMetadata(StageLibraryTask stageLibrary) {
      // Populate the definitions with all the stage definitions
      List<StageDefinition> stageDefinitions = stageLibrary.getStages();

      DefinitionsJson definitions = new DefinitionsJson();
      if (!stageDefinitions.isEmpty()) {
        try (InputStream is = stageDefinitions.get(0).getStageClassLoader().getResourceAsStream("data-collector-library.properties")) {
          Properties properties = new Properties();
          properties.load(is);
          definitions.setVersion(properties.getProperty("cloud.version"));
          definitions.setCategory(properties.getProperty("cloud.category"));
          definitions.setCategoryLabel(properties.getProperty("cloud.category.label"));
          definitions.setExecutorVersion(properties.getProperty("cloud.executor.version"));
        } catch (Exception ex) {
          System.err.printf("ERROR: Could not obtain cloud version/category information: '%s\n'", ex);
          System.exit(1);
        }
      }

      List<StageDefinitionJson> stages = new ArrayList<>(stageDefinitions.size());
      stages.addAll(BeanHelper.wrapStageDefinitions(stageDefinitions));
      definitions.setStages(stages);

      Map<String, Object> map = new HashMap<>();
      map.put(
          EL_FUNCTION_DEFS,
          BeanHelper.wrapElFunctionDefinitionsIdx(ConcreteELDefinitionExtractor.get().getElFunctionsCatalog())
      );
      map.put(
          EL_CONSTANT_DEFS,
          BeanHelper.wrapElConstantDefinitionsIdx(ConcreteELDefinitionExtractor.get().getELConstantsCatalog())
      );
      definitions.setElCatalog(map);

      for (StageDefinition stageDefinition : stageDefinitions) {
        extractIcon(stageDefinition);
        extractYamlUpgrader(stageDefinition);
      }

      return definitions;
    }

  }

}
