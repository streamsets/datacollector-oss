/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.support;

import org.zendesk.client.v2.Zendesk;
import org.zendesk.client.v2.model.Attachment;
import org.zendesk.client.v2.model.Comment;
import org.zendesk.client.v2.model.Ticket;

public class StreamSetsSupportZendeskProvider implements StreamSetsSupportProvider {
  /**
   * Our Zendesk's URL.
   */
  private static final String ZENDESK_URL = "https://streamsets.zendesk.com";

  /**
   * Name of the bundle as it will be displayed in Zendesk UI.
   */
  private static final String BUNDLE_NAME = "supportBundle.zip";

  /**
   * Mime type associated with the bundle.
   */
  private static final String BUNDLE_MIME = "application/zip";

  private Zendesk buildZendeskClient(SupportCredentials credentials) {
    Zendesk.Builder builder = new Zendesk.Builder(ZENDESK_URL)
      .setUsername(credentials.getUsername());

    if(credentials.isUseToken()) {
      builder.setToken(credentials.getPasswordOrToken());
    } else {
      builder.setPassword(credentials.getPasswordOrToken());
    }

    return builder.build();
  }

  private Attachment.Upload upload(Zendesk zd, byte[] supportBundle) {
    return zd.createUpload(BUNDLE_NAME, BUNDLE_MIME, supportBundle);
  }

  @Override
  public String createNewSupportTicket(SupportCredentials credentials, String headline, String commentText, byte[] supportBundle) {
    Zendesk zd = buildZendeskClient(credentials);

    Comment comment;
    if(supportBundle != null) {
      Attachment.Upload upload = upload(zd, supportBundle);
      comment = new Comment(commentText, upload.getToken());
    } else {
      comment = new Comment(commentText);
    }

    Ticket ticket = new Ticket(
      zd.getCurrentUser().getId(),
      headline,
      comment
    );

    Ticket createdTicket = zd.createTicket(ticket);
    return String.valueOf(createdTicket.getId());
  }

  @Override
  public void commentOnExistingSupportTicket(SupportCredentials credentials, String ticketId, String commentText, byte[] supportBundle) {
    Zendesk zd = buildZendeskClient(credentials);

    Comment comment;
    if(supportBundle != null) {
      Attachment.Upload upload = upload(zd, supportBundle);
      comment = new Comment(commentText, upload.getToken());
    } else {
      comment = new Comment(commentText);
    }

    zd.createComment(Integer.parseInt(ticketId), comment);
  }

  @Override
  public String getPublicUrlForSupportTicket(String ticketId) {
    return "https://streamsets.zendesk.com/agent/tickets/" + ticketId;
  }

}
