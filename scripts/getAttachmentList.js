var request = require('request')
var fs = require('fs')

// get  app directory
var appDir = __dirname
var appDirList = appDir.split('/')
appDirList.pop(-1)
appDir = appDirList.join('/')

var page = 0
var returnJson = []

var saveOutput = function (json) {
  fs.writeFile(appDir + '/output/attachments.json', JSON.stringify(json), function (err) {
    if (err) return console.log(err)
  })
}

function isDataDict (attachmentName) {
  var dataDictRegex = new RegExp('datadict', 'i')
  if (dataDictRegex.test(attachmentName)) {
    return true
  }
  return false
}

var getData = function () {
  page += 1

  request({
    url: 'https://data.sfgov.org/api/search/views.json',
    qs: {limit: 200, page: page}
  }, function (err, response, body) {
    if (err) {
      console.log(err)
    }
    var resp = JSON.parse(body)
    if (resp.results) {
      // /console.log(resp.results.length)
      for (var result of resp.results) {
        var view = result.view
        if (view.metadata && view.metadata.attachments && view.metadata.attachments.length > 0) {
          for (var att of view.metadata.attachments) {
            var attId = att.assetId !== '' ? att.assetId : att.blobId
            var filename = att.filename.replace(/\\/g, '|')
            returnJson.push({
              'dataset_id': view.id,
              'attachment_id': attId,
              'attachment_name': filename,
              'attachment_url': 'https://data.sfgov.org/api/views/' + view.id + '/files/' + attId + '?download=true&filename=' + att.name,
              'name': view.name,
              'url': 'https://data.sfgov.org/d/' + view.id,
              'data_dictionary_attached': isDataDict(filename)
            })
          }
        }
      }
      getData()
    } else {
      saveOutput(returnJson)
    }
  })
}
getData()
