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
package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.stagelibrary.StageLibraryUtils;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

public abstract class StageLibraryDefinitionExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(StageLibraryDefinitionExtractor.class);

  public static final String DATA_COLLECTOR_LIBRARY_PROPERTIES = "data-collector-library.properties";

  private static final StageLibraryDefinitionExtractor EXTRACTOR = new StageLibraryDefinitionExtractor() {};

  public static StageLibraryDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public List<ErrorMessage> validate(ClassLoader classLoader) {
    List<ErrorMessage> errors = new ArrayList<>();
    String libraryName = StageLibraryUtils.getLibraryName(classLoader);
    try (InputStream inputStream = classLoader.getResourceAsStream(DATA_COLLECTOR_LIBRARY_PROPERTIES)) {
      if (inputStream != null) {
        new Properties().load(inputStream);
      } else {
        LOG.warn(DefinitionError.DEF_400.getMessage(), libraryName, DATA_COLLECTOR_LIBRARY_PROPERTIES);
      }
    } catch (IOException ex) {
      errors.add(new ErrorMessage(DefinitionError.DEF_401, libraryName, DATA_COLLECTOR_LIBRARY_PROPERTIES,
                                  ex.toString()));
    }
    return errors;
  }

  @SuppressWarnings("unchecked")
  public StageLibraryDefinition extract(ClassLoader classLoader) {
    List<ErrorMessage> errors = validate(classLoader);
    if (errors.isEmpty()) {
      String libraryName = StageLibraryUtils.getLibraryName(classLoader);
      String libraryLabel = StageLibraryUtils.getLibraryLabel(classLoader);
      Properties libraryProps = new Properties();
      try (InputStream inputStream = classLoader.getResourceAsStream(DATA_COLLECTOR_LIBRARY_PROPERTIES)) {
        if (inputStream != null) {
          libraryProps.load(inputStream);
        }
      } catch (IOException ex) {
        throw new RuntimeException(Utils.format("Unexpected exception: {}", ex.toString()), ex);
      }

      List<Class> elClasses = new ArrayList<>();
      try {
        Enumeration<URL> resources = classLoader.getResources(StageLibraryTask.EL_DEFINITION_RESOURCE);
        while (resources.hasMoreElements()) {
          URL url = resources.nextElement();
          try (InputStream is = url.openStream()) {
            List<String> elList = ObjectMapperFactory.get().readValue(is, List.class);
            for (String className : elList) {
              Class<? extends Stage> klass = (Class<? extends Stage>) classLoader.loadClass(className);
              elClasses.add(klass);
            }
          }
        }
      } catch (IOException | ClassNotFoundException ex) {
        throw new RuntimeException(
            Utils.format("Could not load EL definitions from '{}', {}", classLoader, ex.toString()), ex);
      }

      Class[] elClassesArr = elClasses.toArray(new Class[elClasses.size()]);
      Object contextMsg = Utils.formatL("Stage library [{}] EL definitions", classLoader);
      List<ElFunctionDefinition> functionDefs = ConcreteELDefinitionExtractor.get().extractFunctions(elClassesArr, contextMsg);
      List<ElConstantDefinition> constantDefs = ConcreteELDefinitionExtractor.get().extractConstants(elClassesArr, contextMsg);

      return new StageLibraryDefinition(classLoader, libraryName, libraryLabel, libraryProps, elClassesArr,
                                        functionDefs, constantDefs);
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid Stage library: {}", errors));
    }
  }
}
