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
 * Controller for Field Selector Modal dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('FieldSelectorModalInstanceController', function ($scope, $timeout, $modalInstance, previewService,
            currentSelectedPaths, activeConfigInfo, detailPaneConfig) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      showLoading: true,
      noPreviewRecord: false,
      recordObject: {},
      selectedPath: _.reduce(currentSelectedPaths, function(obj, path){
        obj[path] = true;
        return obj;
      }, {}),

      save: function() {
        var selectedFieldPaths = [];
        angular.forEach($scope.selectedPath, function(value, key) {
          if(value === true) {
            selectedFieldPaths.push(key);
          }
        });

        $modalInstance.close(selectedFieldPaths);
      },

      close: function() {
        $modalInstance.dismiss('cancel');
      }
    });

    $timeout(function() {
      previewService.getInputRecordsFromPreview(activeConfigInfo.pipelineId, detailPaneConfig, 1).
        then(
        function (inputRecords) {
          $scope.showLoading = false;
          if(_.isArray(inputRecords) && inputRecords.length) {
            $scope.recordObject = inputRecords[0];
            $scope.recordObject.expand = true;
          } else {
            $scope.noPreviewRecord = true;
          }
        },
        function(res) {
          $scope.showLoading = false;
          $scope.common.errors = [res.data];
        }
      );
    }, 300);
  });
