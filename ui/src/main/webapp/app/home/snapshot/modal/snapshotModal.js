/**
 * Controller for Snapshots Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('SnapshotModalInstanceController', function ($scope, $modalInstance, pipelineConfig,
                                                           api, $timeout) {
    var defaultSnapshotName = 'Snapshot1',
      snapshotBatchSize = 10,
      captureSnapshotStatusTimer;

    angular.extend($scope, {
      common: {
        errors: []
      },
      snapshotsInfo: [],
      showLoading: true,
      snapshotInProgress: false,

      /**
       * Capture Snapshot
       */
      captureSnapshot: function() {
        var snapshotName = getNewSnapshotName();
        api.pipelineAgent.captureSnapshot(snapshotName, snapshotBatchSize).
          then(function() {
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
       * Delete Snapshot
       *
       * @param snapshotName
       * @param index
       */
      deleteSnapshot: function(snapshotName, index) {
        $scope.snapshotsInfo.splice(index, 1);
        api.pipelineAgent.deleteSnapshot(pipelineConfig.info.name, snapshotName).
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
      }
    });


    var refreshSnapshotsInfo = function() {
      api.pipelineAgent.getSnapshotsInfo().then(function(res) {
        if(res && res.data && res.data.length) {
          $scope.snapshotsInfo = res.data.sort(function(a, b){
            return a.captured < b.captured;
          });
        }
        $scope.showLoading = false;

      }, function(res) {
        $scope.showLoading = false;
        $scope.common.errors = [res.data];
      });
    };

    var getNewSnapshotName = function() {
      if($scope.snapshotsInfo.length) {
        var lastSnapshot = $scope.snapshotsInfo[0],
          lastName = lastSnapshot ? lastSnapshot.snapshotName : '0',
          indexStrArr = lastName.match(/\d+/),
          index = indexStrArr.length ? parseInt(indexStrArr[0]) : 0;

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
          api.pipelineAgent.getSnapshotStatus(snapshotName)
            .success(function(data) {
              if(data && data.snapshotInProgress === false) {
                $scope.snapshotInProgress = false;
                refreshSnapshotsInfo();
              } else {
                checkForCaptureSnapshotStatus(snapshotName);
              }
            })
            .error(function(data, status, headers, config) {
              $scope.common.errors = [data];
            });
        },
        function() {
          console.log( "Timer rejected!" );
        }
      );
    };

    refreshSnapshotsInfo();
  });