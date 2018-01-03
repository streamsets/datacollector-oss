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
package com.streamsets.datacollector.restapi;


import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.classpath.ClasspathValidatorResult;
import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.config.LineagePublisherDefinition;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.definition.ELDefinitionExtractor;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.execution.alerts.DataRuleEvaluator;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.DefinitionsJson;
import com.streamsets.datacollector.restapi.bean.PipelineDefinitionJson;
import com.streamsets.datacollector.restapi.bean.PipelineRulesDefinitionJson;
import com.streamsets.datacollector.restapi.bean.StageDefinitionJson;
import com.streamsets.datacollector.restapi.bean.StageLibraryExtrasJson;
import com.streamsets.datacollector.restapi.bean.StageLibraryJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.impl.Utils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


@Path("/v1")
@Api(value = "definitions")
@DenyAll
@RequiresCredentialsDeployed
public class StageLibraryResource {
  private static final String DEFAULT_ICON_FILE = "PipelineDefinition-bundle.properties";
  private static final String PNG_MEDIA_TYPE = "image/png";
  private static final String SVG_MEDIA_TYPE = "image/svg+xml";
  private static final String STAGE_LIB_PREFIX = "stage-lib.";
  private static final String NIGHTLY_URL = "http://nightly.streamsets.com/datacollector/";
  private static final String ARCHIVES_URL = "http://archives.streamsets.com/datacollector/";
  private static final String STAGE_LIB_MANIFEST_PATH = "stage-lib-manifest.properties";
  private static final String LATEST = "latest";
  private static final String SNAPSHOT = "-SNAPSHOT";
  private static final String REPO_URL = "REPO_URL";
  private static final String TARBALL_PATH = "/tarball/";
  private static final String TGZ_FILE_EXTENSION = ".tgz";
  private static final String STREAMSETS_LIBS_PATH = "/streamsets-libs/";
  private static final String STREAMSETS_ROOT_DIR_PREFIX = "streamsets-datacollector-";
  private static final String STAGE_LIB_JARS_DIR = "lib";
  private static final String STAGE_LIB_CONF_DIR = "etc";

  @VisibleForTesting
  static final String STAGES = "stages";
  @VisibleForTesting
  static final String PIPELINE = "pipeline";
  @VisibleForTesting
  static final String RULES_EL_METADATA = "rulesElMetadata";
  @VisibleForTesting
  static final String EL_CONSTANT_DEFS = "elConstantDefinitions";
  @VisibleForTesting
  static final String EL_FUNCTION_DEFS = "elFunctionDefinitions";
  @VisibleForTesting
  static final String RUNTIME_CONFIGS = "runtimeConfigs";

  @VisibleForTesting
  static final String EL_CATALOG = "elCatalog";

  private final StageLibraryTask stageLibrary;
  private final BuildInfo buildInfo;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public StageLibraryResource(StageLibraryTask stageLibrary, BuildInfo buildInfo, RuntimeInfo runtimeInfo) {
    this.stageLibrary = stageLibrary;
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
  }

  @GET
  @Path("/definitions")
  @ApiOperation(value = "Returns pipeline & stage configuration definitions", response = DefinitionsJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getDefinitions() {
    //The definitions to be returned
    DefinitionsJson definitions = new DefinitionsJson();

    //Populate the definitions with all the stage definitions
    List<StageDefinition> stageDefinitions = stageLibrary.getStages();
    List<StageDefinitionJson> stages = new ArrayList<>(stageDefinitions.size());
    stages.addAll(BeanHelper.wrapStageDefinitions(stageDefinitions));
    definitions.setStages(stages);

    //Populate the definitions with the PipelineDefinition
    List<PipelineDefinitionJson> pipeline = new ArrayList<>(1);
    pipeline.add(BeanHelper.wrapPipelineDefinition(stageLibrary.getPipeline()));
    definitions.setPipeline(pipeline);

    // Populate service definitions
    List<ServiceDefinition> serviceDefinitions = stageLibrary.getServiceDefinitions();
    definitions.setServices(BeanHelper.wrapServiceDefinitions(serviceDefinitions));

    //Populate the definitions with the PipelineRulesDefinition
    List<PipelineRulesDefinitionJson> pipelineRules = new ArrayList<>(1);
    pipelineRules.add(BeanHelper.wrapPipelineRulesDefinition(stageLibrary.getPipelineRules()));
    definitions.setPipelineRules(pipelineRules);

    definitions.setRulesElMetadata(DataRuleEvaluator.getELDefinitions());

    Map<String, Object> map = new HashMap<>();
    map.put(EL_FUNCTION_DEFS,
        BeanHelper.wrapElFunctionDefinitionsIdx(ELDefinitionExtractor.get().getElFunctionsCatalog()));
    map.put(EL_CONSTANT_DEFS,
        BeanHelper.wrapElConstantDefinitionsIdx(ELDefinitionExtractor.get().getELConstantsCatalog()));
    definitions.setElCatalog(map);

    definitions.setRuntimeConfigs(RuntimeEL.getRuntimeConfKeys());

    return Response.ok().type(MediaType.APPLICATION_JSON).entity(definitions).build();
  }

  @GET
  @Path("/definitions/stages/{library}/{stageName}/icon")
  @ApiOperation(value = "Return stage icon for library and stage name", response = Object.class,
      authorizations = @Authorization(value = "basic"))
  @Produces({SVG_MEDIA_TYPE, PNG_MEDIA_TYPE})
  @PermitAll
  public Response getIcon(@PathParam("library") String library, @PathParam("stageName") String name) {
    StageDefinition stage = Utils.checkNotNull(stageLibrary.getStage(library, name, false),
        Utils.formatL("Could not find stage library: {}, name: {}", library, name));
    String iconFile = DEFAULT_ICON_FILE;
    String responseType = SVG_MEDIA_TYPE;

    if(stage.getIcon() != null && !stage.getIcon().isEmpty()) {
      iconFile = stage.getIcon();
    }

    final InputStream resourceAsStream = stage.getStageClassLoader().getResourceAsStream(iconFile);

    if(iconFile.endsWith(".svg"))
      responseType = SVG_MEDIA_TYPE;
    else if(iconFile.endsWith(".png"))
      responseType = PNG_MEDIA_TYPE;

    return Response.ok().type(responseType).entity(resourceAsStream).build();
  }

  @GET
  @Path("/stageLibraries/list")
  @ApiOperation(value = "Return list of libraries", response = Object.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response getLibraries(
      @QueryParam("repoUrl") String repoUrl,
      @QueryParam("installedOnly") boolean installedOnly
  ) throws IOException {
    List<StageLibraryJson> installedLibraries = new ArrayList<>();
    List<StageLibraryJson> libraries = new ArrayList<>();

    List<StageDefinition> stageDefinitions = stageLibrary.getStages();
    Map<String, Boolean> installedLibrariesMap = new HashMap<>();
    for(StageDefinition stageDefinition: stageDefinitions) {
      if (!installedLibrariesMap.containsKey(stageDefinition.getLibrary())) {
        installedLibrariesMap.put(stageDefinition.getLibrary(), true);
        installedLibraries.add(new StageLibraryJson(
            stageDefinition.getLibrary(),
            stageDefinition.getLibraryLabel(),
            true
        ));
      }
    }

    List<CredentialStoreDefinition> credentialStoreDefinitions = stageLibrary.getCredentialStoreDefinitions();
    for(CredentialStoreDefinition credentialStoreDefinition: credentialStoreDefinitions) {
      if (!installedLibrariesMap.containsKey(credentialStoreDefinition.getStageLibraryDefinition().getName())) {
        installedLibrariesMap.put(credentialStoreDefinition.getStageLibraryDefinition().getName(), true);
        installedLibraries.add(new StageLibraryJson(
            credentialStoreDefinition.getStageLibraryDefinition().getName(),
            credentialStoreDefinition.getStageLibraryDefinition().getLabel(),
            true
        ));
      }
    }

    List<LineagePublisherDefinition> lineagePublisherDefinitions = stageLibrary.getLineagePublisherDefinitions();
    for(LineagePublisherDefinition lineagePublisherDefinition: lineagePublisherDefinitions) {
      if (!installedLibrariesMap.containsKey(lineagePublisherDefinition.getLibraryDefinition().getName())) {
        installedLibrariesMap.put(lineagePublisherDefinition.getLibraryDefinition().getName(), true);
        installedLibraries.add(new StageLibraryJson(
            lineagePublisherDefinition.getLibraryDefinition().getName(),
            lineagePublisherDefinition.getLibraryDefinition().getLabel(),
            true
        ));
      }
    }

    if (!installedOnly) {
      if (repoUrl == null || repoUrl.isEmpty()) {
        String version = buildInfo.getVersion();
        repoUrl = ARCHIVES_URL + version + TARBALL_PATH;
        if (version.contains(SNAPSHOT)) {
          repoUrl = NIGHTLY_URL + LATEST + TARBALL_PATH;
        }
      } else if (!repoUrl.endsWith("/")) {
        repoUrl = repoUrl + "/";
      }
      String url = repoUrl + STAGE_LIB_MANIFEST_PATH;

      Response response = null;
      try {
        response = ClientBuilder.newClient()
            .target(url)
            .request()
            .get();

        InputStream inputStream =  response.readEntity(InputStream.class);
        Properties properties = new Properties();
        properties.load(inputStream);

        for (final String name: properties.stringPropertyNames()) {
          if (name.startsWith(STAGE_LIB_PREFIX)) {
            String libraryId = name.replace(STAGE_LIB_PREFIX, "");
            libraries.add(new StageLibraryJson(
                libraryId,
                properties.getProperty(name),
                installedLibrariesMap.containsKey(libraryId)
            ));
          }
        }
      } finally {
        if (response != null) {
          response.close();
        }
      }
    } else {
      libraries = installedLibraries;
    }

    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(libraries)
        .header(REPO_URL, repoUrl)
        .build();
  }

  @POST
  @Path("/stageLibraries/install")
  @ApiOperation(value = "Install Stage libraries", response = Object.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response installLibraries(
      @QueryParam("repoUrl") String repoUrl,
      List<String> libraryList
  ) throws IOException {
    String runtimeDir = runtimeInfo.getRuntimeDir();
    String version = buildInfo.getVersion();

    for (String libraryId : libraryList) {
      Response response = null;

      if (repoUrl == null || repoUrl.isEmpty()) {
        repoUrl = ARCHIVES_URL + version + TARBALL_PATH;
        if (version.contains(SNAPSHOT)) {
          repoUrl = NIGHTLY_URL + LATEST + TARBALL_PATH;
        }
      } else if (!repoUrl.endsWith("/")) {
        repoUrl = repoUrl + "/";
      }

      String tarFileURL = repoUrl + libraryId + "-" + version + TGZ_FILE_EXTENSION;
      try {
        response = ClientBuilder.newClient()
            .target(tarFileURL)
            .request()
            .get();

        String directory = runtimeDir + "/..";
        String [] runtimeDirStrSplitArr = runtimeDir.split("/");
        String installDirName = runtimeDirStrSplitArr[runtimeDirStrSplitArr.length - 1];
        String tarDirRootName = STREAMSETS_ROOT_DIR_PREFIX + version;

        InputStream inputStream =  response.readEntity(InputStream.class);

        TarArchiveInputStream myTarFile = new TarArchiveInputStream(new GzipCompressorInputStream(inputStream));

        TarArchiveEntry entry = myTarFile.getNextTarEntry();
        while (entry != null) {
          if (entry.isDirectory()) {
            entry = myTarFile.getNextTarEntry();
            continue;
          }
          File curFile = new File(directory, entry.getName().replace(tarDirRootName, installDirName));
          File parent = curFile.getParentFile();
          if (!parent.exists() && !parent.mkdirs()) {
            // Failed to created directory
            throw new RuntimeException(Utils.format("Failed to create directory: {}", parent.getPath()));
          }
          OutputStream out = new FileOutputStream(curFile);
          IOUtils.copy(myTarFile, out);
          out.close();
          entry = myTarFile.getNextTarEntry();
        }
        myTarFile.close();

      } finally {
        if (response != null) {
          response.close();
        }
      }
    }

    return Response.ok().build();
  }

  @POST
  @Path("/stageLibraries/uninstall")
  @ApiOperation(value = "Uninstall Stage libraries", response = Object.class,
      authorizations = @Authorization(value = "basic"))
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response uninstallLibraries(
      List<String> libraryList
  ) throws IOException {
    String runtimeDir = runtimeInfo.getRuntimeDir();
    for (String libraryId : libraryList) {
      if (!libraryId.matches("[a-zA-Z0-9_-]+")) {
        throw new RuntimeException(Utils.format(ContainerError.CONTAINER_01301.getMessage(), libraryId));
      }

      File libraryDirectory = new File(runtimeDir + STREAMSETS_LIBS_PATH + libraryId);
      if (libraryDirectory.exists()) {
        FileUtils.deleteDirectory(libraryDirectory);
      }
    }
    return Response.ok().build();
  }

  @GET
  @Path("/stageLibraries/extras/list")
  @ApiOperation(value = "Return list of additional drivers", response = Object.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response getExtras() throws IOException {
    String libsExtraDir = runtimeInfo.getLibsExtraDir();
    if (StringUtils.isEmpty(libsExtraDir)) {
      throw new RuntimeException(ContainerError.CONTAINER_01300.getMessage());
    }

    List<StageLibraryExtrasJson> extrasList = new ArrayList<>();
    List<StageDefinition> stageDefinitions = stageLibrary.getStages();
    Map<String, Boolean> installedLibrariesMap = new HashMap<>();
    for(StageDefinition stageDefinition: stageDefinitions) {
      if (!installedLibrariesMap.containsKey(stageDefinition.getLibrary())) {
        installedLibrariesMap.put(stageDefinition.getLibrary(), true);
        File stageLibExtraDir = new File(libsExtraDir, stageDefinition.getLibrary());
        if (stageLibExtraDir.exists()) {
          File extraJarsDir = new File(stageLibExtraDir, STAGE_LIB_JARS_DIR);
          addExtras(extraJarsDir, stageDefinition.getLibrary(), extrasList);
          File extraEtc = new File(stageLibExtraDir, STAGE_LIB_CONF_DIR);
          addExtras(extraEtc, stageDefinition.getLibrary(), extrasList);
        }
      }
    }
    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(extrasList)
        .build();
  }

  private void addExtras(File extraJarsDir, String libraryId, List<StageLibraryExtrasJson> extrasList)
      throws IOException {
    if (extraJarsDir != null && extraJarsDir.exists()) {
      File[] files = extraJarsDir.listFiles();
      if (files != null ) {
        for( File f : files){
          extrasList.add(new StageLibraryExtrasJson(f.getAbsolutePath(), libraryId, f.getName()));
        }
      }
    }
  }

  @POST
  @Path("/stageLibraries/extras/{library}/upload")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Install additional drivers", response = Object.class,
      authorizations = @Authorization(value = "basic"))
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response installExtras(
      @PathParam("library") String library,
      @FormDataParam("file") InputStream uploadedInputStream,
      @FormDataParam("file") FormDataContentDisposition fileDetail
  ) throws IOException {
    String libsExtraDir = runtimeInfo.getLibsExtraDir();
    if (StringUtils.isEmpty(libsExtraDir)) {
      throw new RuntimeException(ContainerError.CONTAINER_01300.getMessage());
    }

    File additionalLibraryFile = new File(
        libsExtraDir + "/"	+ library + "/" + STAGE_LIB_JARS_DIR,
        fileDetail.getFileName()
    );
    File parent = additionalLibraryFile.getParentFile();
    if (!parent.exists()) {
      if (!parent.mkdirs()) {
        throw new RuntimeException(Utils.format("Failed to create directory: {}", parent.getName()));
      }
    }
    saveFile(uploadedInputStream, additionalLibraryFile);
    return Response.ok().build();
  }

  private void saveFile(InputStream uploadedInputStream, File additionalLibraryFile) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(additionalLibraryFile)) {
      IOUtils.copy(uploadedInputStream, outputStream);
    }
  }

  @POST
  @Path("/stageLibraries/extras/delete")
  @ApiOperation(value = "Delete additional drivers", response = Object.class,
      authorizations = @Authorization(value = "basic"))
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response deleteExtras(
      List<StageLibraryExtrasJson> extrasList
  ) throws IOException {
    String libsExtraDir = runtimeInfo.getLibsExtraDir();
    if (StringUtils.isEmpty(libsExtraDir)) {
      throw new RuntimeException(ContainerError.CONTAINER_01300.getMessage());
    }
    for (StageLibraryExtrasJson extrasJson : extrasList) {
      File additionalLibraryFile = new File(libsExtraDir + "/"	+
          extrasJson.getLibraryId() + "/" + STAGE_LIB_JARS_DIR, extrasJson.getFileName());
      if (additionalLibraryFile.exists()) {
        FileUtils.forceDelete(additionalLibraryFile);
      }
    }
    return Response.ok().build();
  }

  @GET
  @Path("/stageLibraries/classpathHealth")
  @ApiOperation(
    value = "Validate health of classpath of all loaded stages.",
    response = Object.class,
    authorizations = @Authorization(value = "basic")
  )
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  @Produces(MediaType.APPLICATION_JSON)
  public Response classpathHealth() {
    List<ClasspathValidatorResult> results = stageLibrary.validateStageLibClasspath();
    return Response.ok().entity(results).build();
  }
}
