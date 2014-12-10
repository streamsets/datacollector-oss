/**
 * Controller for Error Alert.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('ErrorAlertController', function ($scope, $modal) {

    angular.extend($scope, {
      /**
       * Display stack trace in modal dialog.
       *
       * @param errorObj
       */
      showStackTrace: function (errorObj) {
        $modal.open({
          templateUrl: 'errorModalContent.html',
          controller: 'ErrorModalInstanceController',
          size: 'lg',
          backdrop: true,
          resolve: {
            errorObj: function () {
              return errorObj;
            }
          }
        });
      }
    });
  });