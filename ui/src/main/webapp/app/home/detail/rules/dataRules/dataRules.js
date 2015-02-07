/**
 * Controller for Data Rules tab.
 */

angular
  .module('dataCollectorApp.home')

  .controller('DataRulesController', function ($scope, $modal, pipelineConstant) {
    angular.extend($scope, {
      showLoading: false,

      /**
       * Callback function for Create New Data Rule button.
       */
      createDataRule: function() {
        if($scope.selectedType === pipelineConstant.LINK) {

          var modalInstance = $modal.open({
            templateUrl: 'app/home/detail/rules/dataRules/editDataRule.tpl.html',
            controller: 'CreateDataRuleModalInstanceController',
            size: 'lg',
            backdrop: 'static',
            resolve: {
              edge: function () {
                return $scope.selectedObject;
              }
            }
          });

          modalInstance.result.then(function (newDataRuleDefn) {
            $scope.pipelineRules.dataRuleDefinitions.push(newDataRuleDefn);
          }, function () {

          });
        }
      },

      /**
       * Callback function for Edit Data Rule button.
       */
      editDataRule: function(dataRuleDefn, index) {
        if($scope.selectedType === pipelineConstant.LINK) {

          var modalInstance = $modal.open({
            templateUrl: 'app/home/detail/rules/dataRules/editDataRule.tpl.html',
            controller: 'EditDataRuleModalInstanceController',
            size: 'lg',
            backdrop: 'static',
            resolve: {
              dataRuleDefn: function () {
                return angular.copy(dataRuleDefn);
              }
            }
          });

          modalInstance.result.then(function (newDataRuleDefn) {
            //$scope.pipelineRules.dataRuleDefinitions[index] = newDataRuleDefn;
            angular.copy(newDataRuleDefn, dataRuleDefn);
          }, function () {

          });
        }
      },

      /**
       * Remove Callback function
       *
       * @param ruleList
       * @param $index
       */
      removeRule: function(ruleList, $index) {
        ruleList.splice($index, 1);
      }
    });

  })

  .controller('CreateDataRuleModalInstanceController', function ($scope, $modalInstance, $translate, edge) {

    angular.extend($scope, {
      showLoading: false,
      common: {
        errors: []
      },
      dataRuleDefn: {
        id: edge.outputLane + (new Date()).getTime(),
        label: '',
        lane: edge.outputLane,
        condition: '',
        samplingPercentage: 5,
        samplingRecordsToRetain: 10,
        alertEnabled: true,
        alertText: '',
        thresholdType: 'COUNT',
        thresholdValue: '100',
        minVolume: 1000,
        sendEmail: false,
        meterEnabled: true,
        enabled: false
      },

      save : function () {
        $modalInstance.close($scope.dataRuleDefn);
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

    $scope.$broadcast('show-errors-check-validity');
  })

  .controller('EditDataRuleModalInstanceController', function ($scope, $modalInstance, $translate, dataRuleDefn) {

    angular.extend($scope, {
      showLoading: false,
      common: {
        errors: []
      },
      dataRuleDefn: dataRuleDefn,

      save : function () {
        $modalInstance.close($scope.dataRuleDefn);
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

    $scope.$broadcast('show-errors-check-validity');
  });