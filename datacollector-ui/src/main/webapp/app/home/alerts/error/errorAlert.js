/**
 * Controller for Error Alert.
 */

angular
  .module('dataCollectorApp.home')
  .controller('ErrorAlertController', function ($scope, $modal) {

    angular.extend($scope, {

      /**
       * Remove Error Message.
       *
       * @param errorList
       * @param index
       */
      removeAlert: function(errorList, index) {
        errorList.splice(index, 1);
      },

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