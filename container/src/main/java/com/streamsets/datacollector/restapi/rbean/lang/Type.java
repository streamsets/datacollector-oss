/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.restapi.rbean.lang;

import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public enum Type {

  RBOOLEAN(true, ImmutableSet.of(RBoolean.class)),
  RCHAR(true, ImmutableSet.of(RChar.class)),
  RDATE(true, ImmutableSet.of(RDate.class)),
  RDATETIME(true, ImmutableSet.of(RDatetime.class)),
  RDECIMAL(true, ImmutableSet.of(RDecimal.class)),
  RDOUBLE(true, ImmutableSet.of(RDouble.class)),
  RENUM(true, ImmutableSet.of(REnum.class)),
  RLONG(true, ImmutableSet.of(RLong.class)),
  RSTRING(true, ImmutableSet.of(RString.class)),
  RTEXT(true, ImmutableSet.of(RText.class)),
  RTIME(true, ImmutableSet.of(RTime.class)),

  RAW(false, ImmutableSet.of()),

  BEAN(true, ImmutableSet.of()),
  LIST(false, ImmutableSet.of()),
  MAP(false, ImmutableSet.of()),
  DYNAMIC(true, ImmutableSet.of()),
  ;

  private static final Map<Class<? extends RType>, Type> CLASS_TYPE_CROSS_REF;

  static {
    CLASS_TYPE_CROSS_REF = new HashMap<>();
    for (Type type : values()) {
      for (Class<? extends RType> klass : type.typeClasses) {
        CLASS_TYPE_CROSS_REF.put(klass, type);
      }
    }
  }

  private boolean rType;
  private final Set<Class<? extends RType>> typeClasses;

  Type(boolean rType, Set<Class<? extends RType>> typeClasses) {
    this.typeClasses = typeClasses;
  }

  public boolean isRType() {
    return rType;
  }

}
