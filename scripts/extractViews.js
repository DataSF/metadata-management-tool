var request = require('request')
var fs = require('fs')

var limitArray = ['TABLES']
var limitSize = limitArray.length
var results = []

for (var i = 0; i < limitSize; i++) {
  request({
    url: 'https://data.sfgov.org/api/search/views.json',
    qs: {limit: 1000, limitTo: limitArray[i]}
  }, function (error, response, body) {
    if (error) {
      return console.log(error)
    }
    results = results.concat(JSON.parse(body).results)
    fs.writeFile('tables.json', JSON.stringify(results), function (err) {
      if (err) return console.log(err)
    })
  })
}
