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
var request = require('request');
var samplePipeline = require('./../../../sample.json');

request({
  url: 'http://localhost:18630/login?j_username=admin&j_password=admin',
  method: "GET",
  json: true,
  headers: {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-Requested-By": "CSRF"
  }
}, function (error, response, body){
  var cookie = response.headers['set-cookie'];

  for (var i =0; i< 10; i++) {
    var pipelineName = "Pipeline " + (i + 1);
    var label = 'table' + (((i + 1) % 10) + 1);
    createTestPipeline(cookie, pipelineName, label);
  }

});

var createTestPipeline = function(cookie, pipelineName, label) {
  request({
    url: 'http://localhost:18630/rest/v1/pipeline/' + pipelineName,
    method: "PUT",
    json: true,
    headers: {
      "Content-Type": "application/json",
      "Accept": "application/json",
      "Cookie": cookie,
      "X-Requested-By": "CSRF"
    }
  }, function (error, response, body){
    console.log(body.uuid);
    var pipelineJSON = samplePipeline.pipelineConfig;

    //console.log(pipelineJSON);
    pipelineJSON.uuid = body.uuid;
    pipelineJSON.description = 'Changed pipeline description';
    pipelineJSON.metadata = {
      labels: [label]
    };

    request({
      url: 'http://localhost:18630/rest/v1/pipeline/' + pipelineName,
      method: "POST",
      json: true,
      body: pipelineJSON,
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Cookie": cookie,
        "X-Requested-By": "CSRF"
      }
    }, function (error, response, body){
      console.log(response);
    });

  });
};
