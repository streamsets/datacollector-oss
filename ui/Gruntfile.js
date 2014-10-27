module.exports = function(grunt) {

  /**
   * Load required Grunt tasks. These are installed based on the versions listed
   * in `package.json` when you do `npm install` in this directory.
   */
  grunt.loadNpmTasks('grunt-contrib-clean');
  grunt.loadNpmTasks('grunt-contrib-copy');
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-contrib-concat');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-uglify');
  grunt.loadNpmTasks('grunt-contrib-less');
  grunt.loadNpmTasks('grunt-karma');
  grunt.loadNpmTasks('grunt-ng-annotate');
  grunt.loadNpmTasks('grunt-html2js');

  var userConfig = {
    /**
     * The `build_dir` folder is where our projects are compiled.
     */
    build_dir: 'dist',
    base_dir: 'src/main/webapp/',

    /**
     * This is a collection of file patterns that refer to our app code (the
     * stuff in `src/`). These file paths are used in the configuration of
     * build tasks. `js` is all project javascript, less tests. `ctpl` contains
     * our reusable components' (`src/common`) template HTML files, while
     * `atpl` contains the same, but for our app's code. `html` is just our
     * main HTML file, `less` is our main stylesheet, and `unit` contains our
     * app's unit tests.
     */
    app_files: {
      js: [ 'app/**/*.js', '!app/**/*.spec.js'],
      jsunit: [ 'app/**/*.spec.js' ],
      atpl: [ 'app/**/*.tpl.html' ],
      ctpl: [ 'common/**/*.tpl.html' ],
      html: [ 'index.html' ],
      less: 'less/app.less',
      i18n: ['i18n/*.json']
    },

    /**
     * This is a collection of files used during testing only.
     */
    test_files: {
      js: ['bower_components/angular-mocks/angular-mocks.js']
    },

    /**
     * This is the same as `app_files`, except it contains patterns that
     * reference vendor code (`bower_components/`) that we need to place into the build
     * process somewhere. While the `app_files` property ensures all
     * standardized files are collected for compilation, it is the user's job
     * to ensure non-standardized (i.e. vendor-related) files are handled
     * appropriately in `vendor_files.js`.
     *
     * The `vendor_files.js` property holds files to be automatically
     * concatenated and minified with our project source files.
     *
     * The `vendor_files.css` property holds any CSS files to be automatically
     * included in our app.
     *
     * The `vendor_files.assets` property holds any assets to be copied along
     * with our app's assets. This structure is flattened, so it is not
     * recommended that you use wildcards
     */
    vendor_files: {
      js: [
        'bower_components/d3/d3.js',
        'bower_components/jquery/dist/jquery.js',
        'bower_components/angular/angular.js',
        'bower_components/angular-route/angular-route.js',
        'bower_components/angular-cookies/angular-cookies.js',
        'bower_components/angular-translate/angular-translate.js',
        'bower_components/angular-translate-storage-cookie/angular-translate-storage-cookie.js',
        'bower_components/angular-translate-loader-static-files/angular-translate-loader-static-files.js',
        'bower_components/angular-dynamic-locale/src/tmhDynamicLocale.js',
        'bower_components/bootstrap/dist/js/bootstrap.js',
        'bower_components/ng-tags-input/ng-tags-input.js',
        'bower_components/json-formatter/dist/json-formatter.js'
      ],
      css: [
        'bower_components/ng-tags-input/ng-tags-input.css',
        'bower_components/json-formatter/dist/json-formatter.css'
      ],
      assets: [
      ],
      fonts: [
        'bower_components/bootstrap/dist/fonts/glyphicons-halflings-regular.eot',
        'bower_components/bootstrap/dist/fonts/glyphicons-halflings-regular.svg',
        'bower_components/bootstrap/dist/fonts/glyphicons-halflings-regular.ttf',
        'bower_components/bootstrap/dist/fonts/glyphicons-halflings-regular.woff'
      ],
      i18n: [
        'bower_components/angular-i18n/*.js'
      ]
    }
  };

  /**
   * This is the configuration object Grunt uses to give each plugin its
   * instructions.
   */
  var taskConfig = {
    /**
     * We read in our `package.json` file so we can access the package name and
     * version. It's already there, so we don't repeat ourselves here.
     */
    pkg: grunt.file.readJSON("package.json"),

    /**
     * The banner is the comment that is placed at the top of our compiled
     * source files. It is first processed as a Grunt template, where the `<%=`
     * pairs are evaluated based on this very configuration object.
     */
    meta: {
      banner:
        '/**\n' +
        ' * <%= pkg.name %> - v<%= pkg.version %> - <%= grunt.template.today("yyyy-mm-dd") %>\n' +
        ' * <%= pkg.homepage %>\n' +
        ' */\n'
    },

    /**
     * The directories to delete when `grunt clean` is executed.
     */
    clean: [
      '<%= base_dir %><%= build_dir %>'
    ],

    /**
     * The `copy` task just copies files from A to B. We use it here to copy
     * our project assets (images, fonts, etc.) and javascripts into
     * `build_dir`, and then to copy the assets to `compile_dir`.
     */
    copy: {
      build_app_assets: {
        files: [
          {
            src: [ '**' ],
            dest: '<%= base_dir %><%= build_dir %>/assets/',
            cwd: '<%= base_dir %>assets',
            expand: true
          }
        ]
      },
      build_vendor_assets: {
        files: [
          {
            src: [ '<%= vendor_files.assets %>' ],
            dest: '<%= base_dir %><%= build_dir %>/assets/',
            cwd: '<%= base_dir %>',
            expand: true,
            flatten: true
          }
        ]
      },
      build_vendor_fonts: {
        files: [
          {
            src: [ '<%= vendor_files.fonts %>' ],
            dest: '<%= base_dir %><%= build_dir %>/fonts',
            cwd: '<%= base_dir %>',
            expand: true,
            flatten: true
          }
        ]
      },
      build_appjs: {
        files: [
          {
            src: [ '<%= app_files.js %>', '<%= app_files.i18n %>' ],
            dest: '<%= base_dir %><%= build_dir %>/',
            cwd: '<%= base_dir %>',
            expand: true
          }
        ]
      },
      build_vendorjs: {
        files: [
          {
            src: [ '<%= vendor_files.js %>', '<%= vendor_files.i18n %>' ],
            dest: '<%= base_dir %><%= build_dir %>/',
            cwd: '<%= base_dir %>',
            expand: true
          }
        ]
      }
    },


    /**
     * HTML2JS is a Grunt plugin that takes all of your template files and
     * places them into JavaScript files as strings that are added to
     * AngularJS's template cache. This means that the templates too become
     * part of the initial payload as one JavaScript file. Neat!
     */
    html2js: {
      /**
       * These are the templates from `src/app`.
       */
      app: {
        options: {
          base: '<%= base_dir %>'
        },
        src: [ '<%= base_dir %><%= app_files.atpl %>' ],
        dest: '<%= base_dir %><%= build_dir %>/templates-app.js'
      },

      /**
       * These are the templates from `src/common`.
       */
      common: {
        options: {
          base: '<%= base_dir %>'
        },
        src: [ '<%= app_files.ctpl %>' ],
        dest: '<%= base_dir %><%= build_dir %>/templates-common.js'
      }
    },

    /**
     * `grunt concat` concatenates multiple source files into a single file.
     */
    concat: {
      /**
       * The `build_css` target concatenates compiled CSS and vendor CSS
       * together.
       */
      build_css: {
        src: getBuildConcatCSSFiles(),
        dest: '<%= base_dir %><%= build_dir %>/assets/<%= pkg.name %>-<%= pkg.version %>.css'
      },
      /**
       * The `compile_js` target is the concatenation of our application source
       * code and all specified vendor source code into a single file.
       */
      compile_js: {
        options: {
          banner: '<%= meta.banner %>'
        },
        src: getCompileJSFiles(),
        dest: '<%= base_dir %><%= build_dir %>/assets/<%= pkg.name %>-<%= pkg.version %>.js'
      }
    },

    /**
     * `ngAnnotate` annotates the sources before minifying. That is, it allows us
     * to code without the array syntax.
     */
    ngAnnotate: {
      compile: {
        files: [
          {
            src: [ '<%= app_files.js %>' ],
            cwd: '<%= base_dir %><%= build_dir %>',
            dest: '<%= base_dir %><%= build_dir %>',
            expand: true
          }
        ]
      }
    },

    /**
     * Minify the sources!
     */
    uglify: {
      compile: {
        options: {
          banner: '<%= meta.banner %>'
        },
        files: {
          '<%= concat.compile_js.dest %>': '<%= concat.compile_js.dest %>'
        }
      }
    },

    /**
     * `grunt-contrib-less` handles our LESS compilation and uglification automatically.
     * Only our `main.less` file is included in compilation; all other files
     * must be imported from this file.
     */
    less: {
      build: {
        files: {
          '<%= base_dir %><%= build_dir %>/assets/<%= pkg.name %>-<%= pkg.version %>.css': '<%= base_dir %><%= app_files.less %>'
        }
      },
      compile: {
        files: {
          '<%= base_dir %><%= build_dir %>/assets/<%= pkg.name %>-<%= pkg.version %>.css': '<%= base_dir %><%= app_files.less %>'
        },
        options: {
          cleancss: true,
          compress: true
        }
      }
    },

    /**
     * `jshint` defines the rules of our linter as well as which files we
     * should check. This file, all javascript sources, and all our unit tests
     * are linted based on the policies listed in `options`. But we can also
     * specify exclusionary patterns by prefixing them with an exclamation
     * point (!); this is useful when code comes from a third party but is
     * nonetheless inside `src/`.
     */
    jshint: {
      src: [
        '<%= base_dir %>/app/**/*.js'
      ],
      gruntfile: [
        'Gruntfile.js'
      ],
      options: {
        curly: true,
        immed: true,
        newcap: true,
        noarg: true,
        sub: true,
        boss: true,
        eqnull: true
      },
      globals: {}
    },


    /**
     * This task compiles the karma template so that changes to its file array
     * don't have to be managed manually.
     */
    karmaconfig: {
      unit: {
        src: [
          '<%= vendor_files.js %>',
          '<%= build_dir %>/templates-app.js',
          '<%= build_dir %>/templates-common.js',
          '<%= test_files.js %>'
        ],
        cwd: '<%= base_dir %>'
      }
    },

    /**
     * The Karma configurations.
     */
    karma: {
      options: {
        configFile: '<%= base_dir %><%= build_dir %>/karma-conf.js'
      },
      unit: {
        port: 9019,
        background: true
      },
      continuous: {
        singleRun: true
      }
    },

    /**
     * The `index` task compiles the `index.html` file as a Grunt template. CSS
     * and JS files co-exist here but they get split apart later.
     */
    index: {

      /**
       * During development, we don't want to have wait for compilation,
       * concatenation, minification, etc. So to avoid these steps, we simply
       * add all script files directly to the `<head>` of `index.html`. The
       * `src` property contains the list of included files.
       */
      build: {
        cwd: '<%= base_dir %><%= build_dir %>',
        src: [
          '<%= vendor_files.js %>',
          'app/**/*.js',
          'templates-app.js',
          'templates-common.js',
          '<%= vendor_files.css %>',
          'assets/<%= pkg.name %>-<%= pkg.version %>.css'
        ]
      },

      /**
       * When it is time to have a completely compiled application, we can
       * alter the above to include only a single JavaScript and a single CSS
       * file. Now we're back!
       */
      compile: {
        cwd: '<%= base_dir %><%= build_dir %>',
        src: [
          'assets/<%= pkg.name %>-<%= pkg.version %>.js',
          '<%= vendor_files.css %>',
          'assets/<%= pkg.name %>-<%= pkg.version %>.css'
        ]
      }
    },

    /**
     * And for rapid development, we have a watch set up that checks to see if
     * any of the files listed below change, and then to execute the listed
     * tasks when they do. This just saves us from having to type "grunt" into
     * the command-line every time we want to see what we're working on; we can
     * instead just leave "grunt watch" running in a background terminal. Set it
     * and forget it, as Ron Popeil used to tell us.
     *
     * But we don't need the same thing to happen for all the files.
     */
    delta: {
      /**
       * By default, we want the Live Reload to work for all tasks; this is
       * overridden in some tasks (like this file) where browser resources are
       * unaffected. It runs by default on port 35729, which your browser
       * plugin should auto-detect.
       */
      options: {
        livereload: true
      },

      /**
       * When the Gruntfile changes, we just want to lint it. In fact, when
       * your Gruntfile changes, it will automatically be reloaded!
       */
      gruntfile: {
        files: 'Gruntfile.js',
        tasks: [ 'jshint:gruntfile' ],
        options: {
          livereload: false
        }
      },

      /**
       * When our JavaScript source files change, we want to run lint them and
       * run our unit tests.
       */
      jssrc: {
        files: [
          '<%= base_dir %>app/**/*.js'
        ],
        tasks: [ 'jshint:src', 'karma:unit:run', 'copy:build_appjs', 'index:build' ]
      },

      /**
       * When our i18n source files change, we want to copy the file to build directory.
       */
      i18nsrc: {
        files: [
          '<%= base_dir %>i18n/*.json'
        ],
        tasks: [ 'copy:build_appjs' ]
      },

      /**
       * When index.html changes, we need to compile it.
       */
      html: {
        files: [ '<%= base_dir %>index.html' ],
        tasks: [ 'index:build' ]
      },

      /**
       * When our templates change, we only rewrite the template cache.
       */
      tpls: {
        files: [
          '<%= base_dir %><%= app_files.atpl %>',
          '<%= base_dir %><%= app_files.ctpl %>'
        ],
        tasks: [ 'html2js' ]
      },

      /**
       * When the CSS files change, we need to compile and minify them.
       */
      less: {
        files: [ 'src/**/*.less' ],
        tasks: [ 'less:build' ]
      },

      /**
       * When a JavaScript unit test file changes, we only want to lint it and
       * run the unit tests. We don't want to do any live reloading.
       */
      jsunit: {
        files: [
          '<%= app_files.jsunit %>'
        ],
        tasks: [ 'jshint:test', 'karma:unit:run' ],
        options: {
          livereload: false
        }
      }
    }
  };


  grunt.initConfig( grunt.util._.extend( taskConfig, userConfig ) );

  /**
   * In order to make it safe to just compile or copy *only* what was changed,
   * we need to ensure we are starting from a clean, fresh build. So we rename
   * the `watch` task to `delta` (that's why the configuration var above is
   * `delta`) and then add a new task called `watch` that does a clean build
   * before watching for changes.
   */
  grunt.renameTask( 'watch', 'delta' );
  grunt.registerTask( 'watch', [ 'build', 'karma:unit', 'delta' ] );

  /**
   * The default task is to build and compile.
   */
  grunt.registerTask( 'default', [ 'build', 'compile' ] );
  //grunt.registerTask( 'default', [ 'build' ] );

  grunt.registerTask( 'test', []);

  /**
   * The `build` task gets your app ready to run for development and testing.
   */
  grunt.registerTask( 'build', [
    'clean', 'html2js', 'jshint', 'less:build', 'concat:build_css',
    'copy:build_app_assets', 'copy:build_vendor_assets', 'copy:build_vendor_fonts',
    'copy:build_appjs', 'copy:build_vendorjs', 'index:build', 'karmaconfig'
    //,'karma:continuous'
  ]);

  /**
   * The `compile` task gets your app ready for deployment by concatenating and
   * minifying your code.
   */
  grunt.registerTask( 'compile', [
    'less:compile', 'ngAnnotate', 'concat:compile_js', 'uglify', 'index:compile'
  ]);

  /**
   * A utility function to get all app JavaScript sources.
   */
  function filterForJS ( files ) {
    return files.filter( function ( file ) {
      return file.match( /\.js$/ );
    });
  }

  /**
   * A utility function to get all app CSS sources.
   */
  function filterForCSS ( files ) {
    return files.filter( function ( file ) {
      return file.match( /\.css$/ );
    });
  }


  /**
   * A utility function to get all JS Files for compile task.
   */
  function getCompileJSFiles(files, prefix) {
    var compileJSFiles = [],
      baseDir = userConfig.base_dir,
      buildDir = userConfig.build_dir,
      vendorFilesJS = userConfig.vendor_files.js;

    vendorFilesJS.forEach(function(file){
      compileJSFiles.push(baseDir + buildDir + '/' + file);
    });

    compileJSFiles.push('module.prefix');
    compileJSFiles.push(baseDir + buildDir + '/app/**/*.js');
    compileJSFiles.push('<%= html2js.app.dest %>');
    compileJSFiles.push('<%= html2js.common.dest %>');
    compileJSFiles.push('module.suffix');

    return compileJSFiles;
  }

  /**
   * A utility function to get all CSS Files for concat:build task.
   */
  function getBuildConcatCSSFiles() {
    var cssFiles = [],
      baseDir = userConfig.base_dir,
      buildDir = userConfig.build_dir,
      vendorFilesCSS = userConfig.vendor_files.css;

    vendorFilesCSS.forEach(function(file){
      cssFiles.push(baseDir + '/' + file);
    });

    cssFiles.push('<%= base_dir %><%= build_dir %>/assets/<%= pkg.name %>-<%= pkg.version %>.css');

    console.log(cssFiles);

    return cssFiles;

  }

  /**
   * The index.html template includes the stylesheet and javascript sources
   * based on dynamic names calculated in this Gruntfile. This task assembles
   * the list into variables for the template to use and then runs the
   * compilation.
   */
  grunt.registerMultiTask( 'index', 'Process index.html template', function () {
    var dirRE = new RegExp( '^('+grunt.config('build_dir')+'|'+grunt.config('compile_dir')+')\/', 'g' );
    console.log(this.filesSrc);
    var jsFiles = filterForJS( this.filesSrc ).map( function ( file ) {
      return file.replace( dirRE, '' );
    });
    var cssFiles = filterForCSS( this.filesSrc ).map( function ( file ) {
      return file.replace( dirRE, '' );
    });

    grunt.file.copy(grunt.config( 'base_dir' ) +'index.html', grunt.config( 'base_dir' ) + grunt.config( 'build_dir' ) + '/index.html', {
      process: function ( contents, path ) {
        return grunt.template.process( contents, {
          data: {
            scripts: jsFiles,
            styles: cssFiles,
            version: grunt.config( 'pkg.version' )
          }
        });
      }
    });
  });

  /**
   * In order to avoid having to specify manually the files needed for karma to
   * run, we use grunt to manage the list for us. The `karma/*` files are
   * compiled as grunt templates for use by Karma. Yay!
   */
  grunt.registerMultiTask( 'karmaconfig', 'Process karma config templates', function () {
    var jsFiles = filterForJS( this.filesSrc );

    grunt.file.copy( grunt.config( 'base_dir' ) + 'karma/karma-conf.tpl.js', grunt.config( 'base_dir' ) + grunt.config( 'build_dir' ) + '/karma-conf.js', {
      process: function ( contents, path ) {
        return grunt.template.process( contents, {
          data: {
            scripts: jsFiles
          }
        });
      }
    });
  });


};


