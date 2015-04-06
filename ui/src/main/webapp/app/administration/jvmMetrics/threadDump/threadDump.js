/**
 * JVMMetrics module for displaying JVM Metrics page content.
 */

angular
  .module('dataCollectorApp.jvmMetrics')
  .controller('ThreadDumpModalInstanceController', function ($scope, $modalInstance, api) {
    angular.extend($scope, {
      showLoading: true,
      common: {
        errors: []
      },
      threads: [],
      columnSort: {
        sortColumn: 'jvmMetrics.threads.id',
        reverse: false
      },

      close : function () {
        $modalInstance.dismiss('cancel');
      },

      refresh: function() {
        loadThreadDump();
      }
    });

    var loadThreadDump = function() {
      $scope.showLoading = true;
      $scope.threads = [];
      api.admin.getThreadDump().then(function(res) {
        $scope.showLoading = false;
        $scope.threads = res.data;
      }, function(res) {
        $scope.showLoading = false;
      });
    };


    loadThreadDump();

  });