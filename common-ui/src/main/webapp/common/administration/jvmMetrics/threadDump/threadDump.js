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
      sortColumn: 'threadId',
      sortReverse: false,

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
        
        angular.forEach($scope.threads, function(thread) {
          thread.threadId = thread.threadInfo.threadId;
          thread.threadName = thread.threadInfo.threadName;
          thread.threadState = thread.threadInfo.threadState;
          thread.blockedCount = thread.threadInfo.blockedCount;
          thread.waitedCount = thread.threadInfo.waitedCount;
        });
        
      }, function(res) {
        $scope.showLoading = false;
      });
    };


    loadThreadDump();

  });