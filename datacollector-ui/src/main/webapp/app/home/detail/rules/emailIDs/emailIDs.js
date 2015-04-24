/**
 * Controller for Email IDs tab.
 */

angular
  .module('dataCollectorApp.home')

  .controller('EmailIDsController', function ($scope) {
    angular.extend($scope, {
      addToList: function(list) {
        list.push('');
      },

      removeFromList: function(list, index) {
        list.splice(index, 1);
      }
    });
  });