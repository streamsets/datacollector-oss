/**
 * Controller for Preview Configuration Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('PreviewConfigModalInstanceController', function ($scope, $modalInstance, pipelineConfig,
                                                                $timeout, pipelineService) {
    angular.extend($scope, {
      previewConfig: angular.copy(pipelineConfig.uiInfo.previewConfig),
      refreshCodemirror: false,

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

  });