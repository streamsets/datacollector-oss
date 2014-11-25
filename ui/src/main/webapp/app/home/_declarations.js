/**
 * Home module for displaying home page content.
 */

angular
  .module('pipelineAgentApp.home', [
    'ngRoute',
    'ngTagsInput',
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
    'nvd3ChartDirectives'
  ])
  .constant('amTimeAgoConfig', {
    withoutSuffix: true
  });