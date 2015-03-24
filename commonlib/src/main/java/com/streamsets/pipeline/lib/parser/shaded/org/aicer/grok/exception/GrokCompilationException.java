/*
 * Copyright 2014 American Institute for Computing Education and Research Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.parser.shaded.org.aicer.grok.exception;

/**
 * Represents a Compilation Error by the Grok Library
 *
 * @author Israel Ekpo <israel@aicer.org>
 */
public class GrokCompilationException extends GrokException {

  private static final long serialVersionUID = -7178169318219517559L;

  public GrokCompilationException() {
    super();
  }

  public GrokCompilationException(String message) {
    super(message);
  }

  public GrokCompilationException(Throwable cause) {
    super(cause);
  }

  public GrokCompilationException(String message, Throwable cause) {
    super(message, cause);
  }

  public GrokCompilationException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}