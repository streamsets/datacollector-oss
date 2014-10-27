/**
 * Home module for displaying home page content
 */

angular
  .module('pipelineAgentApp.home',[
    'ngRoute',
    'ngTagsInput',
    'jsonFormatter',
    'splitterDirectives',
    'tabDirectives',
    'pipelineGraphDirectives'
  ])
  .config(['$routeProvider', function($routeProvider){
    $routeProvider.when("/",
      {
        templateUrl: "app/home/home.tpl.html",
        controller: "HomeController"
      }
    );
  }])
  .controller('HomeController', function($scope, api){

    //Temp
    $scope.config = {
      source: {
        name: 'File Source'
      }
    };


    api.pipelineAgent.getConfig().success(function (res) {
      //$scope.config = res;
      console.log($scope.config);
    });

    $scope.attributes = [{
      name: 'first',
      type: 'String'
    },{
      name: 'last',
      type: 'String'
    },{
      name: 'ssn',
      type: 'String'
    },{
      name: 'address1',
      type: 'String'
    },{
      name: 'address2',
      type: 'String'
    },{
      name: 'city',
      type: 'String'
    },{
      name: 'state',
      type: 'String'
    },{
      name: 'zip',
      type: 'String'
    },{
      name: 'phone',
      type: 'String'
    }];

    $scope.preview = {
      first: "John",
      last: "Smith",
      ssn: "xxx-xx-xxxx",
      address1: "1234 Main St",
      address2: "APT #567",
      city: "San Francisco",
      state: "CA",
      zip: "94014",
      phone: "650-123-4567"
    };

    $scope.createPipelineAgent = function() {
      $scope.config = {
        source: {
          name: 'File Source'
        }
      };
    };

    $scope.pipelineConfig = [40,100,80,15,25,60,10];

  });