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
/**
 * Unit tests for Controller - SDCDirectoriesModalInstanceController
 */

describe('Controller: SDCDirectoriesModalInstanceController', function() {

  var $rootScope, $scope, $controller, api,
    mockSDCDirectories = {
      runtimeDir: "/Users/madhu/Documents/projects/datacollector/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT",
      configDir: "/Users/madhu/Documents/projects/datacollector/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/etc",
      dataDir: "/Users/madhu/Documents/projects/datacollector/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/data",
      logDir: "/Users/madhu/Documents/projects/datacollector/dist/target/streamsets-datacollector-1.0.0b2-SNAPSHOT/streamsets-datacollector-1.0.0b2-SNAPSHOT/log",
      staticWebDir: "/Users/madhu/Documents/projects/datacollector/ui/target/dist"
    };

  beforeEach(function() {
    module('commonUI');
    module('dataCollectorApp');
  });

  beforeEach(inject(function(_$rootScope_, _$controller_, _api_){
    $rootScope = _$rootScope_;
    $scope = $rootScope.$new();
    $controller = _$controller_;
    api = _api_;

    spyOn(api.admin, 'getSDCDirectories').and.callFake(function() {
      return {
        then: function(callback) {
          return callback({
            data: mockSDCDirectories
          });
        }
      };
    });

    $controller('SDCDirectoriesModalInstanceController', {
      $rootScope : $rootScope,
      $scope: $scope,
      $modalInstance: {},
      api: api
    });
  }));

  it('should call service method getSDCDirectories', function() {
    expect(api.admin.getSDCDirectories).toHaveBeenCalled();
  });

  it('should set the value sdcDirectories', function() {
    expect($scope.sdcDirectories).toEqual(mockSDCDirectories);
  });

});