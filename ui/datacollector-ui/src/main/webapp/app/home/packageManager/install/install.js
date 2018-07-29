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
 * Controller for Stage Library Install Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('InstallModalInstanceController',
      function ($scope, $rootScope, $modalInstance, customRepoUrl, libraryList, api, pipelineConstant) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      libraryList: _.clone(libraryList),
      maprStageLib: false,
      operationStatus: 'incomplete',
      operationStatusMap: {},
      failedLibraries: [],
      errorMap: {},

      install: function(givenLibraries) {
        $scope.operationStatus = 'installing';

        var libraries = givenLibraries || libraryList,
            librariesToInstall = [libraries.shift()],
            library = librariesToInstall[0];

        $scope.operationStatusMap[library.id] = 'installing';
        api.pipelineAgent.installLibraries(customRepoUrl, _.pluck(librariesToInstall, 'id'))
            .then(function () {
              library.installed = true;
              $scope.operationStatusMap[library.id] = 'installed';
              $rootScope.common.trackEvent(
                  pipelineConstant.STAGE_LIBRARY_CATEGORY,
                  pipelineConstant.INSTALL_ACTION,
                  library.label,
                  1
              );

              if (libraries.length > 0) {
                $scope.install(libraries);
              } else {
                $scope.operationStatus = 'complete';
              }
            })
            .catch(function (res) {
              $scope.failedLibraries.push(library);
              $scope.operationStatusMap[library.id] = 'failed';
              $scope.errorMap[library.id] = res.data;

              if (libraries.length > 0) {
                $scope.install(libraries);
              } else {
                $scope.operationStatus = 'complete';
              }
            }
          );
      },

      retry: function() {
        var libraries = $scope.failedLibraries;

        $scope.errorMap = {};
        $scope.common.errors = [];
        $scope.failedLibraries = [];

        $scope.install(libraries);
      },

      restart: function() {
        $scope.operationStatus = 'restarting';
        api.admin.restartDataCollector();
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      },

      inStatus: function(library, status) {
        return $scope.operationStatusMap[library.id] === status;
      },

      showError: function(library) {
        var err = $scope.errorMap[library.id];
        $scope.common.errors = [err];
      },

      hasError: function(library) {
        return !!$scope.errorMap[library.id];
      },

      hasErrors: function() {
        return _.any($scope.errorMap);
      }
    });

    if (libraryList && libraryList.length) {
      angular.forEach(libraryList, function(library) {
        if (library.id.indexOf('streamsets-datacollector-mapr_') !== -1) {
          $scope.maprStageLib = true;
        }
      });
    }

  });
