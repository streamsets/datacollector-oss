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

class _PipelineException extends Exception {
  private static final Logger LOG = LoggerFactory.getLogger(_PipelineException.class);

  private static class ExceptionContext {
    private final String bundleName;
    private final ClassLoader classLoader;

    private ExceptionContext(String bundleName, ClassLoader classLoader) {
      this.bundleName = (bundleName != null) ? bundleName + "-bundle" : null;
      this.classLoader = _ApiUtils.checkNotNull(classLoader, "classLoader");
    }

    public String getBundleName() {
      return bundleName;
    }

    public ClassLoader getClassLoader() {
      return classLoader;
    }
  }

  private static final ThreadLocal<ExceptionContext> EXCEPTION_CONTEXT_TL = new ThreadLocal<ExceptionContext>();

  static void setContext(String bundleName, ClassLoader stageClassLoader) {
    EXCEPTION_CONTEXT_TL.set(new ExceptionContext(bundleName, stageClassLoader));
  }

  static boolean isContextSet() {
    return EXCEPTION_CONTEXT_TL.get() != null;
  }

  static void resetContext() {
    EXCEPTION_CONTEXT_TL.remove();
  }

  private String defaultBundle;
  private ErrorId id;
  private Object[] params;
  private ExceptionContext exceptionContext;

  protected _PipelineException(String defaultBundle, ErrorId id, Object... params) {
    super(null, getCause(_ApiUtils.checkNotNull(params, "params")));
    this.defaultBundle = _ApiUtils.checkNotNull(defaultBundle, "defaultBundle");
    this.id = _ApiUtils.checkNotNull(id, "id");
    this.params = params.clone();
    exceptionContext = EXCEPTION_CONTEXT_TL.get();
  }

  public ErrorId getErrorId() {
    return id;
  }

  public String getMessage() {
    return _ApiUtils.format(id.getMessageTemplate(), params);
  }

  public String getMessage(Locale locale) {
    locale = (locale != null) ? locale : Locale.getDefault();
    ResourceBundle rb = null;
    String bundleName = (exceptionContext != null) ? exceptionContext.getBundleName() : null;
    String msg;
    if (bundleName != null) {
      try {
        rb = ResourceBundle.getBundle(bundleName, locale, exceptionContext.getClassLoader());
      } catch (MissingResourceException ex) {
        // setting an exception to create a stack trace
        LOG.warn("Cannot find resource bundle '{}'", bundleName, new Exception());
      }
    }
    String key = id.toString();
    if (rb == null || !rb.containsKey(key)) {
      if (rb != null) {
        LOG.warn("ResourceBundle '{}' does not contain ErrorId '{}'", bundleName, id.getClass() + ":" + id.toString());
      }
      bundleName = defaultBundle;
      try {
        rb = ResourceBundle.getBundle(bundleName, locale, getClass().getClassLoader());
      } catch (MissingResourceException ex) {
        // setting an exception to create a stack trace
        LOG.warn("Cannot find resource bundle '{}'", bundleName, new Exception());
      }
    }
    if (rb != null && rb.containsKey(key)) {
      msg = _ApiUtils.format(rb.getString(key), params);
    } else {
      msg = getMessage();
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
