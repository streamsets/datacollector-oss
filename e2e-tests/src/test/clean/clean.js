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

frisby.create('Login to StreamSets Data Collector')
  .get(browser.baseUrl + 'login?j_username=admin&j_password=admin')
  .expectStatus(200)
  .expectHeader('Content-Type', 'text/html')
  .after(function(body, res) {
    var cookie = res.headers['set-cookie'],
      initialPipelineCount = 0;

    /**
     * GET rest/v1/pipeline-library
     */
    frisby.create('Should return all Pipeline Configuration Info.')
      .get(browser.baseUrl + 'rest/v1/pipelines', {
        headers:  {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Cookie": cookie
        }
      })
      .inspectJSON()
      .expectStatus(200)
      .expectHeaderContains('content-type', 'application/json')
      .afterJSON(function(pipelineListJSON) {
        expect(pipelineListJSON).toBeDefined();

        pipelineListJSON.forEach(function(pipelineInfo) {
          frisby.create('Should be able to delete pipeline configuration.')
            .delete(browser.baseUrl + 'rest/v1/pipeline/' + pipelineInfo.name, {}, {
              headers:  {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Cookie": cookie,
                "X-Requested-By": "CSRF"
              }
            })
            .expectStatus(200)
            .toss();
        });

      })
      .toss();


  })
  .toss();