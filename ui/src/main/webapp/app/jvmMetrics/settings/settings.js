/**
 * JVMMetrics Settings module for displaying JVM Metrics page content.
 */

angular
  .module('dataCollectorApp.jvmMetrics')
  .controller('JVMMetricsSettingsModalInstanceController', function ($scope, $modalInstance, availableCharts, selectedCharts) {
    angular.extend($scope, {
      showLoading: false,
      common: {
        errors: []
      },
      availableCharts: availableCharts,
      selectedCharts: {
        selected : selectedCharts
      },

      save : function () {
        $modalInstance.close($scope.selectedCharts.selected);
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

    $scope.$broadcast('show-errors-check-validity');
  });