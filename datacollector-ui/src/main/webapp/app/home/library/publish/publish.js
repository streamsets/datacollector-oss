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
 * Controller for Publish Pipeline to  Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('PublishModalInstanceController', function ($scope, $modalInstance, pipelineInfo, api, $q, authService) {
    angular.extend($scope, {
      remoteBaseUrl: authService.getRemoteBaseUrl(),
      publishing: false,
      common: {
        errors: []
      },
      commitPipelineModel : {
        name: _.isArray(pipelineInfo) ? undefined : pipelineInfo.title,
        commitMessage: ''
      },
      isList: _.isArray(pipelineInfo),
      publish : function () {
        $scope.publishing = true;
        if ($scope.isList) {
          var deferList = [];
          for (var i = 0; i < pipelineInfo.length; i++) {
            deferList.push(api.controlHub.publishPipeline(
              pipelineInfo[i].pipelineId,
              $scope.commitPipelineModel.commitMessage
            ));
          }
          $q.all(deferList).then(
            function() {
              $modalInstance.close();
            },
            function(res) {
              if (res && res.status === 403) {
                $scope.common.errors =
                  ['User Not Authorized. Contact your administrator to request permission to use this functionality'];
              } else {
                $scope.common.errors = [res.data];
              }
              $scope.publishing = false;
            }
          );
        } else {
          api.controlHub.publishPipeline(
            pipelineInfo.pipelineId,
            $scope.commitPipelineModel.commitMessage
          ).then(
            function(res) {
              $modalInstance.close(res.data.metadata);
            },
            function(res) {
              if (res && res.status === 403) {
                $scope.common.errors =
                  ['User Not Authorized. Contact your administrator to request permission to use this functionality'];
              } else {
                $scope.common.errors = [res.data];
              }
              $scope.publishing = false;
            }
          );
        }
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });
  });
