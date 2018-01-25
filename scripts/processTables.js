var request = require('request')
var fs = require('fs')
// get app dir
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

const fnPath = appDir + '/output/tables.json'


let offset = 0


//callback function: after we get 
function writeResultsFile (allResults){
  fs.writeFile(fnPath, JSON.stringify(allResults), function (err) {
          if (err) return console.log(err)
  })
  console.log('We are done');
}


var allResults = []

function getData(loopOffset, callback){
console.log("starting the requests")

request({
  url: 'https://data.sfgov.org/api/search/views.json',
  qs: {limit: 1000, page: loopOffset}
}, function (error, response, body) {
  if (error) {
    console.log(error)
  } else {
    var results = JSON.parse(body).results
    if (results){
      console.log("got some results")
    //fs.writeFile(appDir + '/output/input.json', JSON.stringify(results), function (err) {
    //  if (err) return console.log(err)
    //})
    results = results.filter(function (result) {
      return result.view.flags && (result.view.viewType === 'tabular' || result.view.viewType === 'geo')
    })
      .map(function (result, index, arr) {
        if (result.view.viewType === 'tabular') {
          var columns = result.view.columns.map(function (column, index, arr) {
            var col = {
              'columnID': result.view.id + '_' + column.fieldName,
              'internalColumnID': column.id,
              'systemID': result.view.id,
              'data_type': result.view.viewType,
              'dataset_name': result.view.name,
              'createdAt': new Date(result.view.createdAt * 1000),
              'rowsUpdatedAt': new Date(result.view.rowsUpdatedAt * 1000),
              'viewLastModified': new Date(result.view.viewLastModified * 1000),
              'indexUpdatedAt': new Date(result.view.indexUpdatedAt * 1000),
              'childView': result.view.childViews ? result.view.childViews[0] : null,
              'department': result.view.metadata && result.view.metadata.custom_fields && result.view.metadata.custom_fields['Department Metrics'] ? result.view.metadata.custom_fields['Department Metrics']['Publishing Department'] : '',
              'field_name': column.name,
              'field_type': column.dataTypeName,
              'field_render_type': mapColumnTypes(column.renderTypeName),
              'field_description': column.description,
              'field_api_name': column.fieldName
            }
            return col
          }
          )
          return columns
        } else {
          var ret = {
            'internalColumnID': '',
            'systemID': result.view.id,
            'data_type': result.view.viewType,
            'dataset_name': result.view.name,
            'createdAt': new Date(result.view.createdAt),
            'rowsUpdatedAt': new Date(result.view.rowsUpdatedAt),
            'viewLastModified': new Date(result.view.viewLastModified),
            'indexUpdatedAt': new Date(result.view.indexUpdatedAt),
            'childView': result.view.childViews ? result.view.childViews[0] : null,
            'department': result.view.metadata && result.view.metadata.custom_fields ? result.view.metadata.custom_fields['Department Metrics']['Publishing Department'] : '',
            'field_name': '',
            'field_type': '',
            'field_render_type': '',
            'field_description': '',
            'field_api_name': ''
          }
          return ret
        }
      }).reduce(function (prev, curr) {
        return prev.concat(curr)
      })
      loopOffset = loopOffset + 1
      //console.log(loopOffset)
      //console.log(allResults.length)
      allResults = allResults.concat(results)
      //console.log(allResults.length)
      getData(loopOffset, callback)
    }else {
      console.log(allResults.length)
      callback(allResults)
      //return allResults
    }
  }
})
}


//get the data from views.json, then write data to file as the callback function
getData(offset, writeResultsFile)

