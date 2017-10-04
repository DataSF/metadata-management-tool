var request = require('request')
var fs = require('fs')

var appDir = __dirname
var appDirList = appDir.split('/')
appDirList.pop(-1)
appDir = appDirList.join('/')

function mapColumnTypes (dataTypeName) {
  var lookupDict = {
    'text': 'text',
    'number': 'numeric',
    'calendar_date': 'timestamp',
    'checkbox': 'boolean',
    'money': 'numeric',
    'location': 'geometry: point',
    'date': 'timestamp',
    'polygon': 'geometry: polygon',
    'multipolygon': 'geometry: multipolygon',
    'percent': 'numeric',
    'url': 'text',
    'line': 'geometry: line',
    'document': 'blob',
    'point': 'geometry: point',
    'html': 'text',
    'drop_down_list': 'text',
    'phone': 'text',
    'photo': 'blob',
    'multipoint': 'geometry: multipoint',
    'multiline': 'geometry: multiline'
  }
  let renderDataType = lookupDict[dataTypeName]
  return renderDataType
}

fs.readFile(appDir + '/output/tables.json', function (err, data) {
  if (err) {
    console.log(err)
  }

  data = JSON.parse(data)

  var geoFields = []

  var geoData = data.filter(function (row) {
    return row.data_type === 'geo'
  })

  var getView = function (data, iter, count, cb) {
    var row = data[count]
    count += 1
    request({
      url: 'https://data.sfgov.org/api/views/' + row.childView + '.json'
    }, function (err, response, body) {
      if (err) {
        console.log(err)
      }
      var result = JSON.parse(body)
      // /console.log('https://data.sfgov.org/api/views/' + row.childView + '.json')
      if (result.columns) {
        // console.log(result.columns)
        var columns = result.columns.map(function (column, index, arr) {
          // console.log(column)
        if (column.fieldName !== '') {
          return {
              'data_type': 'geo',
              'dataset_name': result.name,
              'columnID': row.systemID + '_' + column.fieldName,
              'internalColumnID': column.id,
              'systemID': row.systemID,
              'createdAt': result.createdAt,
              'rowsUpdatedAt': result.rowsUpdatedAt,
              'viewLastModified': result.viewLastModified,
              'indexUpdatedAt': result.indexUpdatedAt,
              'childView': row.childView,
              'department': row.department,
              'field_name': column.name,
              'field_type': column.dataTypeName,
              // 'field_render_type': column.renderTypeName,
              'field_render_type': mapColumnTypes(column.renderTypeName),
              'field_description': column.description,
              'field_api_name': column.fieldName
            }
        } else {
          console.log('********')
          console.log(column)
          }
        })
      }

      geoFields = result.columns ? geoFields.concat(columns) : geoFields

      if (iter === count) {
        cb(geoFields)
      } else {
        getView(data, iter, count, cb)
      }
    })
  }

  var saveOutput = function (array) {
    var output = array.reduce(function (prev, curr) {
      return prev.concat(curr)
    }, [])

    fs.writeFile(appDir + '/output/geo.json', JSON.stringify(output), function (err) {
      if (err) return console.log(err)
    })
  }

  var iterations = geoData.length
  getView(geoData, iterations, 0, saveOutput)
})
