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

// Controller for History.
angular
  .module('dataCollectorApp.home')
  .controller('HistoryController', function ($rootScope, $scope, _, api, pipelineService, $modal) {

    angular.extend($scope, {
      showLoading: false,

      /**
       * Show summary of the pipeline run.
       *
       * @param history
       * @param $index
       */
      viewSummary: function(history, $index) {
        var prevHistory;
        var pipelineStateHistory = $scope.pipelineStateHistory;

        while($index + 1 < $scope.pipelineStateHistory.length) {
          if(pipelineStateHistory[$index + 1].status === 'STARTING') {
            prevHistory = pipelineStateHistory[$index + 1];
            break;
          }
          $index++;
        }

        $modal.open({
          templateUrl: 'app/home/detail/history/summary/summaryModal.tpl.html',
          controller: 'SummaryModalInstanceController',
          size: 'lg',
          backdrop: 'static',
          resolve: {
            pipelineConfig: function() {
              return $scope.pipelineConfig;
            },
            history: function () {
              return history;
            },
            prevHistory: function() {
              return prevHistory;
            }
          }
        });
      },

      /**
       * Clear History Callback function.
       *
       * @param $event
       */
      clearHistory: function($event) {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/detail/history/clearHistory/clearHistory.tpl.html',
          controller: 'ClearHistoryModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return $scope.pipelineConfig.info;
            }
          }
        });

        if($event) {
          $event.stopPropagation();
        }

        modalInstance.result.then(function () {
          $scope.updateHistory($scope.pipelineConfig.info.pipelineId);
        });
      },
      showStackTraceFromParams: function (errorMessage, errorStackTrace) {
        pipelineService.showStackTraceFromParams(errorMessage, errorStackTrace);
      }
    });
  });
