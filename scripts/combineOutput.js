var fs = require('fs')

// get app dir
var appDir = __dirname
var appDirList = appDir.split('/')
appDirList.pop(-1)
appDir = appDirList.join('/')

fs.readFile(appDir + '/output/tables.json', function (err, data) {
  if (err) {
    console.log(err)
  }

  var tables = JSON.parse(data).filter(function (row) {
    return (row.data_type === 'tabular' || row.data_type === 'geo') && row.field_api_name !== ''
  })

  fs.readFile(appDir + '/output/geo.json', function (err, data) {
    if (err) {
      console.log(err)
    }
    var geo = JSON.parse(data)
    var combined = tables.concat(geo)

    fs.writeFile(appDir + '/output/combined.json', JSON.stringify(combined), function (err) {
      if (err) {
        console.log(err)
      }
    })
  })
})
