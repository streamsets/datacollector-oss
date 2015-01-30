/**
 * Controller for Raw Preview.
 */

angular
  .module('dataCollectorApp.home')

  .controller('RawPreviewController', function ($scope, $rootScope, $q, $modal, _, api) {

    angular.extend($scope, {
      /**
       * Raw Source Preview
       */
      rawSourcePreview: function() {
        api.pipelineAgent.rawSourcePreview($scope.activeConfigInfo.name, 0, $scope.detailPaneConfig.uiInfo.rawSource.configuration)
          .success(function(data) {
            $rootScope.common.errors = [];
            $scope.rawSourcePreviewData = data ? data.previewString : '';
          })
          .error(function(data, status, headers, config) {
            $rootScope.common.errors = [data];
          });
      }
    });

  });