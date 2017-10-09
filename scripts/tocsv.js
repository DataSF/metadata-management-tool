var json2csv = require('json2csv')
var fs = require('fs')

var jsonfiles = ['combined', 'attachments']
var appDir = __dirname
var appDirList = appDir.split('/')
appDirList.pop(-1)
appDir = appDirList.join('/')

jsonfiles.forEach(function (file) {
  fs.readFile(appDir + '/output/' + file + '.json', function (err, data) {
    if (err) throw err
    data = JSON.parse(data)
    var csv = json2csv({data: data})
    // console.log(csv)
    fs.writeFile(appDir + '/output/' + file + '.csv', csv, function (err) {
      if (err) throw err
      console.log('file saved')
      console.log('made it here')
    })
  })
})
