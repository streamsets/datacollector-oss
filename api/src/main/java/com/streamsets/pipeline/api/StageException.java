/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class StageException extends Exception {
  private static final Logger LOG = LoggerFactory.getLogger(StageException.class);
  private static final String PIPELINE_BUNDLE_NAME = "pipeline-bundle";

  public interface ID {

    public String getMessageTemplate();

  }

  private static class StageContext {
    private final String bundleName;
    private final ClassLoader classLoader;

    private StageContext(Stage.Info info, ClassLoader stageClassLoader) {
      bundleName = (info.getName() + "-" + info.getVersion() + "-bundle").replace('.', '_');
      classLoader = stageClassLoader;
    }

    public String getBundleName() {
      return bundleName;
    }

    public ClassLoader getClassLoader() {
      return classLoader;
    }
  }

  private static final ThreadLocal<StageContext> STAGE_CONTEXT_TL = new ThreadLocal<StageContext>();

  static void setStageContext(Stage.Info info, ClassLoader stageClassLoader) {
    STAGE_CONTEXT_TL.set(new StageContext(info, stageClassLoader));
  }

  static void resetStageContext() {
    STAGE_CONTEXT_TL.remove();
  }

  private ID id;
  private Object params;
  private StageContext stageContext;

  public StageException(ID id, Object... params) {
    super(null, getCause(_ApiUtils.checkNotNull(params, "params")));
    this.id = _ApiUtils.checkNotNull(id, "id");
    this.params = params.clone();
    stageContext = STAGE_CONTEXT_TL.get();
    if (stageContext == null) {
      // setting an exception to create a stack trace
      LOG.warn("The PipelineException.StageContext has has not been set, messages won't be localized", new Exception());
    }
  }

  public ID getID() {
    return id;
  }

  public String getMessage() {
    return _ApiUtils.format(id.getMessageTemplate(), params);
  }

  public String getMessage(Locale locale) {
    locale = (locale != null) ? locale : Locale.getDefault();
    ResourceBundle rb = null;
    String msg;
    if (stageContext != null) {
      try {
        rb = ResourceBundle.getBundle(stageContext.getBundleName(), locale, stageContext.getClassLoader());
      } catch (MissingResourceException ex) {
        // setting an exception to create a stack trace
        LOG.warn("Cannot find resource bundle '{}'", stageContext.getBundleName());
      }
    }
    if (rb == null) {
      msg = getMessage();
    } else {
      String key = id.toString();
      if (!rb.containsKey(key)) {
        rb = ResourceBundle.getBundle(PIPELINE_BUNDLE_NAME, locale, getClass().getClassLoader());
        if (!rb.containsKey(key)) {
          msg = getMessage();
          LOG.warn("ResourceBundle '{}' does not contain PipelineException.ID '{}'", stageContext.getBundleName(),
                   id.getClass() + ":" + id.toString());
        } else {
          msg = _ApiUtils.format(rb.getString(key), params);
        }
      } else {
        msg = _ApiUtils.format(rb.getString(key), params);
      }
    }
    return msg;
  }

  private static Throwable getCause(Object... params) {
    Throwable throwable = null;
    if (params.length > 0 && params[params.length - 1] instanceof Throwable) {
      throwable = (Throwable) params[params.length - 1];
    }
    return throwable;
  }

}
