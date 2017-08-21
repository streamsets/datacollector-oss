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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.maven.rbgen;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;


@Mojo(name="rbgen", defaultPhase = LifecyclePhase.PACKAGE, requiresDependencyResolution = ResolutionScope.COMPILE)
public class RBGenMojo extends AbstractMojo {
  private static final String BUNDLES_TO_GEN_FILE = "datacollector-resource-bundles.json";
  private static final String STAGE_CLASS_NAME = "com.streamsets.pipeline.api.Stage";
  private static final String ERROR_CODE_CLASS_NAME = "com.streamsets.pipeline.api.ErrorCode";
  private static final String LABEL_CLASS_NAME = "com.streamsets.pipeline.api.Label";
  private static final String STAGE_DEF_CLASS_NAME = "com.streamsets.pipeline.api.StageDef";
  private static final String ERROR_STAGE_CLASS_NAME = "com.streamsets.pipeline.api.ErrorStage";
  private static final String CONFIG_DEF_CLASS_NAME = "com.streamsets.pipeline.api.ConfigDef";

  @Parameter(defaultValue="${project}", readonly=true)
  private MavenProject project;

  @Parameter(defaultValue="${project.build.directory}/resource-bundles")
  private File output;

  private Class stageClass;
  private Class errorCodeClass;
  private Class labelClass;
  private Class stageDefClass;
  private Class errorStageClass;
  private Class configDefClass;

  private boolean usesDataCollectorAPI(ClassLoader classLoader) {
    try {
      stageClass = classLoader.loadClass(STAGE_CLASS_NAME);
      errorCodeClass = classLoader.loadClass(ERROR_CODE_CLASS_NAME);
      labelClass = classLoader.loadClass(LABEL_CLASS_NAME);
      stageDefClass = classLoader.loadClass(STAGE_DEF_CLASS_NAME);
      errorStageClass = classLoader.loadClass(ERROR_STAGE_CLASS_NAME);
      configDefClass = classLoader.loadClass(CONFIG_DEF_CLASS_NAME);
      return true;
    } catch (ClassNotFoundException ex) {
      return false;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void execute() throws MojoExecutionException {
    try {
      ClassLoader projectCL = getProjectClassLoader();
      if (usesDataCollectorAPI(projectCL)) {
        File file = new File(project.getBuild().getOutputDirectory(), BUNDLES_TO_GEN_FILE);
        if (file.exists() && file.isFile()) {
          Map<String, File> bundles = new HashMap<>();
          List<String> classNames = new ObjectMapper().readValue(file, List.class);
          if (!classNames.isEmpty()) {
            for (String e : classNames) {
              Class klass = projectCL.loadClass(e);
              String bundleName = klass.getName().replace(".", "/") + "-bundle.properties";
              File bundleFile = generateDefaultBundleForClass(klass, bundleName);
              bundles.put(bundleName, bundleFile);
            }
            File jarFile = new File(project.getBuild().getDirectory(),
                                    project.getArtifactId() + "-" + project.getVersion() + "-bundles.jar");
            getLog().info("Building bundles jar: " + jarFile.getAbsolutePath());
            createBundlesJar(jarFile, bundles);
          } else {
            getLog().debug(BUNDLES_TO_GEN_FILE + "' file does not have any class, no bundles jar will be generated");
          }
        } else {
          getLog().debug("Project does not have '" + BUNDLES_TO_GEN_FILE + "' file, no bundles jar will be generated");
        }
      } else {
        getLog().debug("Project does not use DataCollector API, no bundles jar will be generated");
      }
    } catch (Throwable ex) {
      throw new MojoExecutionException(ex.toString(), ex);
    }
  }


  private ClassLoader getProjectClassLoader() throws Exception {
    List<String> cp = project.getCompileClasspathElements();
    URL[] urls = new URL[cp.size()];
    for (int i = 0; i < cp.size(); i++) {
      urls[i] = new File(cp.get(i)).toURI().toURL();
    }
    return new URLClassLoader(urls, this.getClass().getClassLoader());
  }

  private File generateDefaultBundleForClass(Class klass, String bundleName) throws Exception {
    File bundleFile = new File(output, bundleName);
    if (!bundleFile.getParentFile().exists()) {
      if (!bundleFile.getParentFile().mkdirs()) {
        throw new IOException("Could not create directory: " + bundleFile.getParentFile());
      }
    }
    // we want to preserve the order, thus we cannot use JDK Properties for this.
    try (Writer writer = new FileWriter(bundleFile)) {
      for (Map.Entry<String, String> entry : extractResources(klass).entrySet()) {
        writer.write(escapeValue(entry.getKey(), true) + "=" + escapeValue(entry.getValue(), false) + "\n");
      }
      getLog().debug("Generated bundle: " + bundleName);
    }
    return bundleFile;
  }

  @SuppressWarnings("unchecked")
  private String invokeMessageMethod(Class klass, String methodName, Object obj) throws Exception {
    Method method = klass.getMethod(methodName);
    return (String) method.invoke(obj);
  }

  @SuppressWarnings("unchecked")
  private LinkedHashMap<String, String> extractResources(Class klass) throws  Exception {
    LinkedHashMap<String, String> map = new LinkedHashMap<>();
    if (klass.isEnum()) {
      Object[] enums = klass.getEnumConstants();
      for (Object e : enums) {
        String name = ((Enum)e).name();
        String text = name;
        if (errorCodeClass.isAssignableFrom(klass)) {
          text = invokeMessageMethod(errorCodeClass, "getMessage", e);
        }
        if (labelClass.isAssignableFrom(klass)) {
          text = invokeMessageMethod(labelClass, "getLabel", e);
        }
        map.put(name, text);
      }
    } else if (stageClass.isAssignableFrom(klass)) {
      Annotation stageDef = klass.getAnnotation(stageDefClass);
      if (stageDef != null) {
        String labelText = invokeMessageMethod(stageDefClass, "label", stageDef);
        map.put("stageLabel", labelText);
        String descriptionText = invokeMessageMethod(stageDefClass, "description", stageDef);
        map.put("stageDescription", descriptionText);
        Annotation errorStage = klass.getAnnotation(errorStageClass);
        if (errorStage != null) {
          String errorLabelText = invokeMessageMethod(errorStageClass, "label", errorStage);
          if (errorLabelText.isEmpty()) {
            errorLabelText = labelText;
          }
          map.put("errorStageLabel", errorLabelText);
          String errorDescriptionText = invokeMessageMethod(errorStageClass, "description", errorStage);
          if (!errorDescriptionText.isEmpty()) {
            errorDescriptionText = descriptionText;
          }
          map.put("errorStageDescription", errorDescriptionText);

        }
      }
      for (Field field : klass.getFields()) {
        Annotation configDef = field.getAnnotation(configDefClass);
        if (configDef != null) {
          String labelText = invokeMessageMethod(configDefClass, "label", configDef);
          map.put("configLabel." + field.getName(), labelText);
          String descriptionText = invokeMessageMethod(configDefClass, "description", configDef);
          map.put("configDescription." + field.getName(), descriptionText);
        }
      }

    }
    return map;
  }

  // same escaping done by java.util.Properties for keys/values

  /*
   * Escapes special characters with a preceding slash
   */
  private String escapeValue(String str, boolean escapeSpace) {
    int len = str.length();
    int bufLen = len * 2;
    if (bufLen < 0) {
      bufLen = Integer.MAX_VALUE;
    }
    StringBuilder outBuffer = new StringBuilder(bufLen);

    for(int x=0; x<len; x++) {
      char aChar = str.charAt(x);
      // Handle common case first, selecting largest block that
      // avoids the specials below
      if ((aChar > 61) && (aChar < 127)) {
        if (aChar == '\\') {
          outBuffer.append('\\'); outBuffer.append('\\');
          continue;
        }
        outBuffer.append(aChar);
        continue;
      }
      switch(aChar) {
        case ' ':
          if (x == 0 || escapeSpace)
            outBuffer.append('\\');
          outBuffer.append(' ');
          break;
        case '\t':outBuffer.append('\\'); outBuffer.append('t');
          break;
        case '\n':outBuffer.append('\\'); outBuffer.append('n');
          break;
        case '\r':outBuffer.append('\\'); outBuffer.append('r');
          break;
        case '\f':outBuffer.append('\\'); outBuffer.append('f');
          break;
        case '=': // Fall through
        case ':': // Fall through
        case '#': // Fall through
        case '!':
          outBuffer.append('\\'); outBuffer.append(aChar);
          break;
        default:
          outBuffer.append(aChar);
      }
    }
    return outBuffer.toString();
  }

  private void createBundlesJar(File jarFile, Map<String, File> bundles) throws IOException {
    try (JarOutputStream jar = new JarOutputStream(new FileOutputStream(jarFile))) {
      for (Map.Entry<String, File> entry : bundles.entrySet()) {
        addFile(entry.getKey(), entry.getValue(), jar);
      }
    }
  }

  private void addFile(String path, File file, JarOutputStream jar) throws IOException {
    try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
      JarEntry entry = new JarEntry(path);
      entry.setTime(file.lastModified());
      jar.putNextEntry(entry);
      IOUtils.copy(in, jar);
      jar.closeEntry();
    }
  }

}
