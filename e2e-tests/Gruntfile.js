module.exports = function(grunt) {

  grunt.loadNpmTasks('grunt-protractor-runner');
  grunt.loadNpmTasks('grunt-shell');

  grunt.initConfig({
    shell: {
      dockerBuild: {
        command: 'docker build -t streamsets/sdc ../release'
      },
      dockerComposeUp: {
        command: 'docker-compose up -d'
      },
      dockerComposeStop: {
        command: 'docker-compose stop'
      }
    },
    protractor: {
      options: {
        configFile: "src/test/protractor.conf.js",
        keepAlive: true,
        noColor: false,
        args: {}
      },
      e2e: {}
    }
  });

  grunt.registerTask( 'test', ['shell:dockerBuild', 'shell:dockerComposeUp', 'protractor:e2e',
    'shell:dockerComposeStop']);

};


