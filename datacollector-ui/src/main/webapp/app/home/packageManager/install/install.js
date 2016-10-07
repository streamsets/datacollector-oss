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

/**
 * Controller for Stage Library Install Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('InstallModalInstanceController', function ($scope, $modalInstance, libraryList, api) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      libraryList: libraryList,
      operationInProgress: false,
      libraryInstalled: false,
      isRestartInProgress: false,
      maprStageLib: false,
      solr6StageLib: false,

      install: function() {
        $scope.operationInProgress = true;
        api.pipelineAgent.installLibraries(_.pluck(libraryList, 'id'))
          .then(
            function (res) {
              $scope.libraryInstalled = true;
              $scope.operationInProgress = false;
            },
            function (res) {
              $scope.operationInProgress = false;
              $scope.common.errors = [res];
            }
          );
      },

      restart: function() {
        $scope.isRestartInProgress = true;
        api.admin.restartDataCollector();
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      }
    });

    if (libraryList && libraryList.length) {
      angular.forEach(libraryList, function(library) {
        if (library.id.indexOf('streamsets-datacollector-mapr_') !== -1) {
          $scope.maprStageLib = true;
        }

        if (library.id.indexOf('streamsets-datacollector-apache-solr_6') !== -1) {
          $scope.solr6StageLib = true;
        }
      });
    }

  });
