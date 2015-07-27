/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.definition;

import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.stagelibrary.StageLibraryUtils;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class StageLibraryDefinitionExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(StageLibraryDefinitionExtractor.class);

  private static final String DATA_COLLECTOR_LIBRARY_PROPERTIES = "data-collector-library.properties";

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
                                  ex.getMessage()));
    }
    return errors;
  }

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
        throw new RuntimeException(Utils.format("It should not happen: {}", ex.getMessage()), ex);
      }
      return new StageLibraryDefinition(classLoader, libraryName, libraryLabel, libraryProps);
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid Stage library: {}", errors));
    }
  }

}
