/*
 * Copyright 2021 StreamSets Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.streamsets.datacollector.stagelibrary;

import com.google.common.base.Strings;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.RestErrors;
import com.streamsets.datacollector.restapi.bean.RepositoryManifestJson;
import com.streamsets.datacollector.restapi.bean.StageLibrariesJson;
import com.streamsets.datacollector.restapi.bean.StageLibraryManifestJson;
import com.streamsets.datacollector.util.RestException;
import com.streamsets.datacollector.util.Version;
import com.streamsets.pipeline.SDCClassLoader;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class StageLibraryUtil {
  private static final Logger LOG = LoggerFactory.getLogger(StageLibraryUtil.class);

  private static final String NIGHTLY_URL = "http://nightly.streamsets.com/datacollector/";
  private static final String ARCHIVES_URL = "http://archives.streamsets.com/datacollector/";
  private static final String LATEST = "latest";
  private static final String SNAPSHOT = "-SNAPSHOT";
  private static final String TARBALL_PATH = "/tarball/";
  private static final String ENTERPRISE_PATH = "enterprise/";
  private static final String REPOSITORY_MANIFEST_JSON_PATH = "repository.manifest.json";

  private static final String STREAMSETS_LIBS_FOLDER_NAME = "streamsets-libs";
  private static final String STREAMSETS_ROOT_DIR_PREFIX = "streamsets-datacollector-";

  public static final String LEGACY_TARBALL_PATH = "/legacy/";
  public static final String STREAMSETS_LIBS_PATH = "/streamsets-libs/";
  public static final String CONFIG_PACKAGE_MANAGER_REPOSITORY_LINKS = "package.manager.repository.links";

  public static String[] getRepoUrls(String repoLinksStr, String version) {
    String [] repoURLList;
    if (StringUtils.isEmpty(repoLinksStr)) {
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
    return repoURLList;
  }


  public static RepositoryManifestJson getRepositoryManifestFile(String repoUrl) {
    String repoManifestUrl = repoUrl +  REPOSITORY_MANIFEST_JSON_PATH;
    LOG.info("Reading from Repository Manifest URL: " + repoManifestUrl);

    ClientConfig clientConfig = new ClientConfig();
    clientConfig.property(ClientProperties.READ_TIMEOUT, 2000);
    clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 2000);
    RepositoryManifestJson repositoryManifestJson = null;
    String output = "";
    try (Response response = ClientBuilder.newClient(clientConfig).target(repoManifestUrl).request().get()) {
      InputStream inputStream = response.readEntity(InputStream.class);
      output = IOUtils.toString(inputStream);
      repositoryManifestJson = ObjectMapperFactory.get().readValue(output, RepositoryManifestJson.class);
    } catch (Exception ex) {
      LOG.error("Failed to read repository manifest json {} {}", output, ex);
    }
    return repositoryManifestJson;
  }

  public static StageLibraryManifestJson getStageLibraryManifestJson(String stageLibManifestUrl) {
    StageLibraryManifestJson stageLibManifestJson = null;
    try (Response response = ClientBuilder.newClient().target(stageLibManifestUrl).request().get()) {
      InputStream inputStream = response.readEntity(InputStream.class);
      stageLibManifestJson = ObjectMapperFactory.get().readValue(inputStream, StageLibraryManifestJson.class);
    }  catch (Exception ex) {
      LOG.error("Failed to read stage-lib-manifest.json", ex);
    }
    return stageLibManifestJson;
  }

  public static Map<String, String> getLibraryUrlList(
      List<RepositoryManifestJson> repositoryManifestList,
      Version engineVersion,
      boolean withStageLibVersion,
      List<String> libraryIdList
  ) throws RestException {
    Map<String, String> libraryUrlList = new HashMap<>();
    List<RepositoryManifestJson> repoManifestList = repositoryManifestList;
    if (repoManifestList == null) {
      repoManifestList = Collections.emptyList();
    }
    for(RepositoryManifestJson repositoryManifestJson: repoManifestList) {
      for (StageLibrariesJson stageLibrariesJson : repositoryManifestJson.getStageLibraries()) {
        if (stageLibrariesJson.getStageLibraryManifest() != null) {
          String key = stageLibrariesJson.getStageLibraryManifest().getStageLibId();
          String lookupKey = key;
          if (withStageLibVersion) {
            lookupKey += ":" + stageLibrariesJson.getStagelibVersion();
          }

          if (libraryIdList.contains(lookupKey)) {
            StageLibraryManifestJson manifest = stageLibrariesJson.getStageLibraryManifest();

            // Validate minimal required SDC version
            String minSdcVersionString = manifest.getStageLibMinSdcVersion();
            if(!Strings.isNullOrEmpty(minSdcVersionString)) {
              Version minSdcVersion = null;
              try {
                minSdcVersion = new Version(minSdcVersionString);
              } catch (Exception e) {
                LOG.error("Stage library {} version {} min SDC version '{}' is not a valid SDC version",
                    key,
                    stageLibrariesJson.getStagelibVersion(),
                    minSdcVersionString,
                    e
                );
              }


              if(minSdcVersion != null && !engineVersion.isGreaterOrEqualTo(minSdcVersion)) {
                throw new RestException(
                    RestErrors.REST_1000,
                    key,
                    stageLibrariesJson.getStagelibVersion(),
                    minSdcVersionString,
                    engineVersion
                );
              }
            }

            libraryUrlList.put(key, manifest.getStageLibFile());
          }
        }
      }
    }

    // The sizes should fit
    if (libraryUrlList.size() != libraryIdList.size()) {
      Set<String> missingStageLibs = new HashSet<>(libraryIdList);
      missingStageLibs.removeAll(libraryUrlList.keySet());

      throw new RestException(RestErrors.REST_1001, String.join(", ", missingStageLibs));
    }
    return libraryUrlList;
  }


  @FunctionalInterface
  public interface StageLibPresenceValidator {
    void assertStageLibNotPresent(String libId) throws Exception;
  }

  public static void installStageLibs(
      String runtimeDir,
      String version,
      Map<String, String> libraryUrlList,
      StageLibPresenceValidator stageLibPresenceValidator
  ) throws Exception {
    for (Map.Entry<String, String>  libraryEntry : libraryUrlList.entrySet()) {
      String libraryId = libraryEntry.getKey();
      String libraryUrl = libraryEntry.getValue();
      LOG.info("Installing stage library {} from {}", libraryId, libraryUrl);

      try (Response response = ClientBuilder.newClient()
          .target(libraryUrl)
          .request()
          .get()) {

        String runtimeDirParent = runtimeDir + "/..";
        String[] runtimeDirStrSplitArr = runtimeDir.split("/");
        String installDirName = runtimeDirStrSplitArr[runtimeDirStrSplitArr.length - 1];
        String tarDirRootName = STREAMSETS_ROOT_DIR_PREFIX + version;

        InputStream inputStream = response.readEntity(InputStream.class);

        TarArchiveInputStream myTarFile = new TarArchiveInputStream(new GzipCompressorInputStream(inputStream));

        TarArchiveEntry entry = myTarFile.getNextTarEntry();

        String directory = null;

        stageLibPresenceValidator.assertStageLibNotPresent(libraryId);

        while (entry != null) {
          if (entry.isDirectory()) {
            entry = myTarFile.getNextTarEntry();
            if (directory == null) {
              // Initialize root folder
              if (entry.getName().startsWith(STREAMSETS_LIBS_FOLDER_NAME)) {
                directory = runtimeDir;
              } else if (!entry.getName().contains(STREAMSETS_LIBS_FOLDER_NAME)) {
                // legacy stage lib
                directory = Paths.get(runtimeDir, STREAMSETS_LIBS_FOLDER_NAME).toString();
              } else {
                directory = runtimeDirParent;
              }
              LOG.info("Stage Library Install Root Folder is : {}", directory);
            }
            continue;
          }

          File curFile = new File(directory, entry.getName().replace(tarDirRootName, installDirName));
          File parent = curFile.getParentFile();
          if (!parent.exists() && !parent.mkdirs()) {
            // Failed to create directory
            throw new RestException(RestErrors.REST_1003, parent.getPath());
          }
          OutputStream out = new FileOutputStream(curFile);
          IOUtils.copy(myTarFile, out);
          out.close();
          entry = myTarFile.getNextTarEntry();
        }
        myTarFile.close();
      }
    }
  }

  public static SDCClassLoader getStageLibClassLoader(File stagelibJarsDir) throws MalformedURLException {
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

  public static List<SDCClassLoader> getInstalledStageLibClassLoaders(String stageLibDir) throws IOException {
    File[] stageLibFolders = new File(stageLibDir).listFiles(File::isDirectory);
    List<SDCClassLoader> stageLibClassLoaders = new ArrayList<>();
    if (stageLibFolders != null && stageLibFolders.length > 0) {
      for (File stageLibFolder : stageLibFolders) {
        stageLibClassLoaders.add(getStageLibClassLoader(new File(stageLibFolder, "lib")));
      }
    }
    return stageLibClassLoaders;
  }

}
