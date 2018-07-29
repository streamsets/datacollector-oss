/*
 * Copyright 2017 StreamSets Inc.
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
var frisby = require('frisby');

frisby.create('Login to StreamSets Data Collector using admin role')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET rest/v1/admin/threadsDump
     */
    frisby.create('Should return SDC JVM Thread Dump')
      .get(browser.baseUrl + 'rest/v1/system/threads', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(threadsJSON) {
        expect(threadsJSON).toBeDefined();
        expect(threadsJSON.length > 1).toBeTruthy();
        expect(threadsJSON[0].threadInfo.threadName).toBeDefined();
        expect(threadsJSON[0].threadInfo.stackTrace).toBeDefined();
      })
      .toss();


  })
  .toss();



frisby.create('Login to StreamSets Data Collector using creator role')
  .get(browser.baseUrl + 'login?j_username=creator&j_password=creator')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'];

    /**
     * GET rest/v1/admin/threadsDump
     */
    frisby.create('Should throw 403 forbidden error when non admin user requests for SDC JVM Thread Dump')
      .get(browser.baseUrl + 'rest/v1/system/threads', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .expectStatus(403)
      .expectHeaderContains('content-type', 'text/html')
      .toss();


    /**
     * GET rest/v1/admin/shutdown
     */
    frisby.create('Should throw 403 forbidden error when non admin user requests for shutdown')
      .post(browser.baseUrl + 'rest/v1/system/shutdown', {}, {
        headers:  {
          "Cookie": cookie,
          "X-Requested-By": "CSRF"
        }
      })
      .expectStatus(403)
      .expectHeaderContains('content-type', 'text/html')
      .toss();


  })
  .toss();