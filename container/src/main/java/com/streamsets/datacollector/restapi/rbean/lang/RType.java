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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class RType<R extends RType> {
  private static final List<BaseMsg> NO_MESSAGES = Collections.emptyList();

  private List<BaseMsg> messages = NO_MESSAGES;

  @SuppressWarnings("unchecked")
  public R addMessage(BaseMsg message) {
    Preconditions.checkNotNull(message, "message cannot be NULL");
    if (messages == NO_MESSAGES) {
      messages = new ArrayList<>();
    }
    messages.add(message);
    return (R) this;
  }

  @SuppressWarnings("unchecked")
  protected R addMessages(List<? extends BaseMsg> messages) {
    Preconditions.checkNotNull(messages, "messages cannot be NULL");
    for (BaseMsg message : messages) {
      addMessage(message);
    }
    return (R) this;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<BaseMsg> getMessages() {
    return messages;
  }

  public void clearMessages() {
    if (!messages.isEmpty()) {
      messages.clear();
    }
  }

}
