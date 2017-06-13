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
describe('Controller: SummaryModalInstanceController', function() {

  var $rootScope, $scope, $controller,
    pipelineConstant = {
      'PIPELINE': 'Pipeline'
    },
    mockPipeline = {
      info: {
        name: 'Mock Pipeline'
      }
    },
    mockHistory = {
      "name" : "sample",
      "rev" : "0",
      "state" : "STOPPED",
      "message" : "The pipeline was stopped. The last committed source offset is tweets.json::-1.",
      "lastStatusChange" : 1427926573397,
      "metrics" : "{\n  \"version\" : \"3.0.0\",\n  \"gauges\" : { },\n  \"meters\" : {}}"
    };

  beforeEach(module('dataCollectorApp.home'));

  beforeEach(inject(function(_$rootScope_, _$controller_){
    $rootScope = _$rootScope_;
    $scope = $rootScope.$new();
    $controller = _$controller_;

    $controller('SummaryModalInstanceController', {
      $rootScope : $rootScope,
      $scope: $scope,
      pipelineConfig: mockPipeline,
      $modalInstance: {},
      prevHistory: mockHistory,
      history: mockHistory,
      pipelineConstant: pipelineConstant
    });
  }));

  it('should set the value selected type to pipeline', function() {
    expect($scope.selectedType == pipelineConstant.PIPELINE);
  });

  it('should parse the metrics string to object', function() {
    expect($scope.common.pipelineMetrics).toBeDefined();
    expect($scope.common.pipelineMetrics.version).toEqual("3.0.0");
  });

  it('should return name of pipeline when getLabel function is called', function() {
    expect($scope.getLabel()).toEqual('Mock Pipeline');
  });


});