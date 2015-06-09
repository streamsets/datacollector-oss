/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.definition;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.StageLibraryDefinition;
import com.streamsets.pipeline.stagelibrary.StageLibraryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class StageLibraryDefinitionExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(StageLibraryDefinitionExtractor.class);

  private static final String DATA_COLLECTOR_LIBRARY_PROPERTIES = "data-collector-library.properties";

  private static final StageLibraryDefinitionExtractor EXTRACTOR = new StageLibraryDefinitionExtractor() {};

  public static StageLibraryDefinitionExtractor get() {
    return EXTRACTOR;
  }

  public StageLibraryDefinition extract(ClassLoader classLoader) {
    String libraryName = StageLibraryUtils.getLibraryName(classLoader);
    String libraryLabel = StageLibraryUtils.getLibraryLabel(classLoader);
    Properties libraryProps = new Properties();
    try (InputStream inputStream = classLoader.getResourceAsStream(DATA_COLLECTOR_LIBRARY_PROPERTIES)) {
      if (inputStream != null) {
        libraryProps.load(inputStream);
      } else {
        LOG.warn("Stage library '{}', file '{}' not found", libraryName, DATA_COLLECTOR_LIBRARY_PROPERTIES);
      }
    } catch (IOException ex) {
      throw new IllegalArgumentException(Utils.format("Stage library '{}', could not read file '{}': {}",
                                                      libraryName, DATA_COLLECTOR_LIBRARY_PROPERTIES, ex.getMessage()),
                                         ex);
    }
    try {
      return new StageLibraryDefinition(classLoader, libraryName, libraryLabel, libraryProps);
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(Utils.format("Stage library '{}', could not initialize library definition: {}",
                                                      libraryName, ex.getMessage()), ex);
    }
  }

}
