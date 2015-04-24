/**
 * Controller for Success Alert.
 */

angular
  .module('dataCollectorApp.home')
  .controller('SuccessAlertController', function ($scope, $timeout) {

    angular.extend($scope, {
      /**
       * Returns Message of the alert. Also clears the message after 5 seconds.
       * @param alert
       * @param alertList
       * @param index
       * @returns {*}
       */
      getAlertMessage: function(alert, alertList, index) {
        $timeout(function() {
          if(alertList.length > index) {
            $scope.removeAlert(alertList, index);
          }
        }, 4000);

        return alert.message;
      },

      /**
       * Remove Message.
       *
       * @param alertList
       * @param index
       *
       */
      removeAlert: function(alertList, index) {
        alertList.splice(index, 1);
      }
    });
  });