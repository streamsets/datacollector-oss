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

import com.streamsets.datacollector.main.BuildInfo;

/**
 * Interface exposing API that enables users and customers to create or update support tickets at StreamSets.
 */
public interface StreamSetsSupportProvider {

  /**
   * Register build info for this DataCollector.
   *
   * @param buildInfo Build info instance.
   */
  public void setBuildInfo(BuildInfo buildInfo);

  /**
   * Create new support ticket with StreamSets support.
   *
   * @param credentials Credentials for StreamSets support portal
   * @param priority Priority of the ticket to be created
   * @param headline Headline for the ticket
   * @param comment Initial comment describing the problem
   * @param supportBundle Optional bytes that should be uploaded to the support portal as support bundle
   * @return Internal id of the new ticket
   */
  public String createNewSupportTicket(SupportCredentials credentials, TicketPriority priority, String headline, String comment, byte[] supportBundle);


  /**
   * Add a comment to existing support ticket.
   *
   * @param credentials Credentials for StreamSets support portal
   * @param ticketId Id of the ticket in StreamSets suppport system.
   * @param comment Initial comment describing the problem
   * @param supportBundle Optional bytes that should be uploaded to the support portal as support bundle
   */
  public void commentOnExistingSupportTicket(SupportCredentials credentials, String ticketId, String comment, byte[] supportBundle);

  /**
   * Generate an URL for given support ticket. User can follow the URL to see a page with the ticket details.
   *
   * @param ticketId Support ticket id.
   * @return Publicly accessible url (https://..)
   */
  public String getPublicUrlForSupportTicket(String ticketId);
}
