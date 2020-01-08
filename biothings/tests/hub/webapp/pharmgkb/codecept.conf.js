exports.config = {
  tests: './*_test.js',
  output: './output',
  helpers: {
    Puppeteer: {
      url: 'http://localhost:8080',
      show: true,
      chrome: {
        args: ['--no-sandbox', '--window-size=1440,1024'],
        defaultViewport: null,
      },
    },
    customHelper: {
      require: './customHelper.js',
    },
  },
  include: {
    I: './steps_file.js'
  },
  bootstrap: null,
  mocha: {},
  name: 'tests'
}
