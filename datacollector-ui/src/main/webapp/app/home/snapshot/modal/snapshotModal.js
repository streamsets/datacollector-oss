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
 * Controller for Snapshots Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('SnapshotModalInstanceController', function (
    $scope, $rootScope, $modalInstance, pipelineConfig, isPipelineRunning, canExecute, api, $timeout
  ) {
    var defaultSnapshotName = 'Snapshot1',
      snapshotBatchSize = 50000,
      captureSnapshotStatusTimer;

    angular.extend($scope, {
      common: {
        errors: []
      },
      snapshotsInfo: [],
      showLoading: true,
      snapshotInProgress: false,
      isPipelineRunning: isPipelineRunning,
      canExecute: canExecute,

      /**
       * Capture Snapshot
       */
      captureSnapshot: function() {
        var snapshotName = 'snapshot' + (new Date()).getTime();
        var snapshotLabel = getNewSnapshotName();
        api.pipelineAgent.captureSnapshot(
          pipelineConfig.info.pipelineId,
          0,
          snapshotName,
          snapshotLabel,
          snapshotBatchSize,
          !$scope.isPipelineRunning
        ).then(function() {
          if (!isPipelineRunning) {
            $rootScope.$storage.maximizeDetailPane = false;
            $rootScope.$storage.minimizeDetailPane = false;
            $rootScope.$storage.readNotifications = [];
            $rootScope.common.pipelineMetrics = {};
            $scope.isPipelineRunning = true;
          }
          $scope.snapshotsInfo.push({
            name: pipelineConfig.info.pipelineId,
            id: snapshotName,
            label: snapshotLabel,
            inProgress: true
          });
          $scope.snapshotInProgress = true;
          checkForCaptureSnapshotStatus(snapshotName);
        }, function(res) {
          $scope.common.errors = [res.data];
        });

      },

      /**
       * View Snapshot
       *
       * @param snapshotName
       */
      viewSnapshot: function(snapshotName) {
        $modalInstance.close(snapshotName);
      },

      /**
       * Download Snapshot
       *
       * @param snapshotName
       */
      downloadSnapshot: function(snapshotInfo) {
        api.pipelineAgent.downloadSnapshot(snapshotInfo.name, 0, snapshotInfo.id);
      },

      /**
       * Delete Snapshot
       *
       * @param snapshotName
       * @param index
       */
      deleteSnapshot: function(snapshotName, index) {
        $scope.snapshotsInfo.splice(index, 1);
        api.pipelineAgent.deleteSnapshot(pipelineConfig.info.pipelineId, 0, snapshotName).
          then(function() {

          }, function(res) {
            $scope.common.errors = [res.data];
          });
      },

      /**
       * Cancel Snapshot
       *
       * @param snapshotName
       * @param index
       */
      cancelSnapshot: function(snapshotName, index) {
        $scope.snapshotsInfo.splice(index, 1);
        $timeout.cancel(captureSnapshotStatusTimer);
        $scope.snapshotInProgress = false;
        api.pipelineAgent.deleteSnapshot(pipelineConfig.info.pipelineId, 0, snapshotName).
          then(function() {

          }, function(res) {
            $scope.common.errors = [res.data];
          });
      },

      /**
       * Close and Escape Command Handler
       */
      close: function() {
        $modalInstance.dismiss('cancel');
      },

      /**
       * SnapshotInfo Label Update Command handler
       * @param snapshotInfo
       */
      snapshotInfoLabelUpdated: function(snapshotInfo) {
        api.pipelineAgent.updateSnapshotLabel(pipelineConfig.info.pipelineId, 0, snapshotInfo.id, snapshotInfo.label).
        then(function() {

        }, function(res) {
          $scope.common.errors = [res.data];
        });
      }
    });


    var refreshSnapshotsInfo = function() {
      api.pipelineAgent.getSnapshotsInfo().then(function(res) {
        if(res && res.data && res.data.length) {

          $scope.snapshotsInfo = _.chain(res.data)
            .filter(function(snapshotInfo) {
              return snapshotInfo.name === pipelineConfig.info.pipelineId;
            })
            .sortBy('timeStamp')
            .value();

          var snapshotInfoInProgress = _.find($scope.snapshotsInfo, function(snapshotInfo) {
            return snapshotInfo.inProgress;
          });

          if(snapshotInfoInProgress)  {
            $scope.snapshotInProgress = true;
            checkForCaptureSnapshotStatus(snapshotInfoInProgress.id);
          }
        }
        $scope.showLoading = false;

      }, function(res) {
        $scope.showLoading = false;
        $scope.common.errors = [res.data];
      });
    };

    var getNewSnapshotName = function() {
      if($scope.snapshotsInfo.length) {
        var lastSnapshot = $scope.snapshotsInfo[$scope.snapshotsInfo.length - 1];
        var lastName = lastSnapshot ? lastSnapshot.label : '0';
        var indexStrArr = lastName.match(/\d+/);
        var index = indexStrArr && indexStrArr.length ? parseInt(indexStrArr[0]) : 0;
        return 'Snapshot' + (++index);
      }

      return defaultSnapshotName;
    };

    /**
     * Check for Snapshot Status for every 1 seconds, once done open the snapshot view.
     *
     */
    var checkForCaptureSnapshotStatus = function(snapshotName) {
      captureSnapshotStatusTimer = $timeout(
        function() {
          //console.log( "Pipeline Metrics Timeout executed", Date.now() );
        },
        1000
      );

      captureSnapshotStatusTimer.then(
        function() {
          api.pipelineAgent.getSnapshotStatus(pipelineConfig.info.pipelineId, 0, snapshotName)
            .then(function(res) {
              var data = res.data;
              if(data && data.inProgress === false) {
                $scope.snapshotInProgress = false;
                refreshSnapshotsInfo();
              } else {
                checkForCaptureSnapshotStatus(snapshotName);
              }
            })
            .catch(function(res) {
              $scope.common.errors = [res.data];
            });
        },
        function() {
          //console.log( "Timer rejected!" );
        }
      );
    };

    refreshSnapshotsInfo();

    $scope.$on('$destroy', function() {
      $timeout.cancel(captureSnapshotStatusTimer);
    });
  });
