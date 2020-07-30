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
 * Controller for Library Pane Duplicate Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('DuplicateModalInstanceController', function (
    $scope, $modalInstance, pipelineInfo, isSamplePipeline, api, $q
  ) {
    var newTitle = pipelineInfo.title + ' copy';
    if (isSamplePipeline) {
      newTitle = pipelineInfo.title;
    }
    angular.extend($scope, {
      common: {
        errors: []
      },
      newConfig : {
        title: newTitle,
        description: pipelineInfo.description,
        numberOfCopies: 1
      },
      isSamplePipeline: isSamplePipeline,
      operationInProgress: false,

      save : function () {
        if (isSamplePipeline) {
          $scope.operationInProgress = true;
          api.pipelineAgent.getSamplePipeline(pipelineInfo.pipelineId)
            .then(function(res) {
              var pipelineEnvelope = res.data;
              var pipelineObject = pipelineEnvelope.pipelineConfig;
              var pipelineRulesObject = pipelineEnvelope.pipelineRules;
              return api.pipelineAgent.duplicatePipelineConfig(
                $scope.newConfig.title,
                $scope.newConfig.description,
                pipelineObject,
                pipelineRulesObject
              );
            }).then(
            function(configObject) {
              $modalInstance.close(configObject);
            },
            function(res) {
              $scope.operationInProgress = false;
              $scope.common.errors = [res.data];
            }
          );
        } else if ($scope.newConfig.numberOfCopies === 1) {
          $scope.operationInProgress = true;
          $q.all([
            api.pipelineAgent.getPipelineConfig(pipelineInfo.pipelineId),
            api.pipelineAgent.getPipelineRules(pipelineInfo.pipelineId)
          ]).then(function(results) {
            var pipelineObject = results[0].data;
            var pipelineRulesObject = results[1].data;
            return api.pipelineAgent.duplicatePipelineConfig(
              $scope.newConfig.title,
              $scope.newConfig.description,
              pipelineObject,
              pipelineRulesObject
            );
          }).then(
            function(configObject) {
              $modalInstance.close(configObject);
            },
            function(res) {
              $scope.operationInProgress = false;
              $scope.common.errors = [res.data];
            }
          );
        } else {
          $scope.operationInProgress = true;
          $q.all([
            api.pipelineAgent.getPipelineConfig(pipelineInfo.pipelineId),
            api.pipelineAgent.getPipelineRules(pipelineInfo.pipelineId)
          ]).then(function(results) {
            var pipelineObject = results[0].data;
            var pipelineRulesObject = results[1].data;
            var deferList = [];
            for (var i = 0; i < $scope.newConfig.numberOfCopies; i++) {
              deferList.push(
                api.pipelineAgent.duplicatePipelineConfig(
                  $scope.newConfig.title + (i + 1),
                  $scope.newConfig.description,
                  pipelineObject,
                  pipelineRulesObject
                )
              );
            }
            $q.all(deferList)
              .then(
                function(configObjects) {
                  $modalInstance.close(configObjects);
                },
                function(res) {
                  $scope.operationInProgress = false;
                  $scope.common.errors = [res.data];
                }
              );
          },function(res) {
            $scope.operationInProgress = false;
            $scope.common.errors = [res.data];
          });
        }
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });
  });
