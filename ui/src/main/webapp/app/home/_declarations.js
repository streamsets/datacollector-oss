/**
 * Home module for displaying home page content.
 */

angular
  .module('pipelineAgentApp.home', [
    'ngRoute',
    'jsonFormatter',
    'splitterDirectives',
    'tabDirectives',
    'pipelineGraphDirectives',
    'showErrorsDirectives',
    'ngEnterDirectives',
    'contenteditableDirectives',
    'underscore',
    'ui.bootstrap',
    'filereadDirectives',
    'angularMoment',
    'nvd3ChartDirectives',
    'abbreviateNumberFilter',
    'ngSanitize',
    'ui.select',
    'showLoadingDirectives',
    'recordTreeDirectives'
  ])
  .constant('amTimeAgoConfig', {
    withoutSuffix: true
  });