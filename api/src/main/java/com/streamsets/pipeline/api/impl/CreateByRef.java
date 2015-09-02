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
package com.streamsets.pipeline.api.impl;

import java.util.concurrent.Callable;

public class CreateByRef {

  private static final ThreadLocal<Boolean> BY_REF_TL = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  public static boolean isByRef() {
    return BY_REF_TL.get() == Boolean.TRUE;
  }

  public static <T> T call(Callable<T> callable) throws Exception{
    boolean alreadyByRef = BY_REF_TL.get() == Boolean.TRUE;
    try {
      if (!alreadyByRef) {
        BY_REF_TL.set(Boolean.TRUE);
      }
      return callable.call();
    } finally {
      if (!alreadyByRef) {
        BY_REF_TL.set(Boolean.FALSE);
      }
    }
  }

}
