/**
 * Controller for Info Alert.
 */

angular
  .module('dataCollectorApp.home')
  .controller('InfoAlertController', function ($scope) {

    angular.extend($scope, {

      /**
       * Remove Message.
       *
       * @param alertList
       * @param index
       */
      removeAlert: function(alertList, index) {
        alertList.splice(index, 1);
      }
    });
  });