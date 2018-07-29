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
 * Controller for Import From Archive Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('ImportFromArchiveModalInstanceController', function ($scope, $modalInstance, api, $translate) {
    var errorMsg = 'Not a valid Pipeline Configuration file.';

    angular.extend($scope, {
      common: {
        errors: []
      },
      successEntities: [],
      showLoading: true,
      uploadFile: {},
      operationDone: false,
      operationInProgress: false,

      /**
       * Import button callback function.
       */
      import: function () {
        $scope.operationInProgress = true;
        var formData = new FormData();
        formData.append('file', $scope.uploadFile);
        api.pipelineAgent.importPipelines(formData)
          .then(function(response) {
            var res = response.data;
            $scope.common.errors = res.errorMessages;
            $scope.successEntities = res.successEntities;
            $scope.operationDone = true;
            $scope.operationInProgress = false;
          })
          .catch(function(res) {
            $scope.common.errors = [res.data];
            $scope.operationDone = true;
            $scope.operationInProgress = false;
          });
      },

      /**
       * Cancel button callback.
       */
      cancel: function () {
        $modalInstance.dismiss('cancel');
      },

      /**
       * Close button callback.
       */
      close: function () {
        $modalInstance.close($scope.successEntities);
      }
    });

  });
