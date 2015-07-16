/**
 * Controller for Preview Configuration Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('PreviewConfigModalInstanceController', function ($scope, $rootScope, $modalInstance, pipelineConfig,
                                                                $timeout, pipelineService, api, pipelineConstant) {
    angular.extend($scope, {
      previewConfig: angular.copy(pipelineConfig.uiInfo.previewConfig),
      refreshCodemirror: false,
      snapshotsInfo: [],

      /**
       * Returns Codemirror Options
       * @param options
       * @returns {*}
       */
      getCodeMirrorOptions: function(options) {
        return angular.extend({}, pipelineService.getDefaultELEditorOptions(), options);
      },

      /**
       * Run Preview Command Handler
       */
      runPreview: function() {
        pipelineConfig.uiInfo.previewConfig = $scope.previewConfig;
        $modalInstance.close();
      },

      /**
       * Cancel and Escape Command Handler
       */
      cancel: function() {
        $modalInstance.dismiss('cancel');
      }

    });

    $timeout(function() {
      $scope.refreshCodemirror = true;
    });


    if($rootScope.common.sdcExecutionMode !== pipelineConstant.CLUSTER) {
      api.pipelineAgent.getSnapshotsInfo().then(function(res) {
        if(res && res.data && res.data.length) {
          $scope.snapshotsInfo = res.data;
          $scope.snapshotsInfo = _.chain(res.data)
            .filter(function(snapshotInfo) {
              return !snapshotInfo.inProgress;
            })
            .sortBy('id')
            .value();
        }
      }, function(res) {
        $scope.common.errors = [res.data];
      });
    }

  });