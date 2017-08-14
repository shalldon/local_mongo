var csv = require("csv");
var fs = require('fs');
var readline = require('linebyline');

var response = {
  "status": 1000,
  "pagination": null,
  "context": {
    "type": "FeatureCollection",
    "features": []
  }
};


var lines = readline('./files/shipping_points.csv');
lines.on('line', function(line, lineCount, byteCount) {
  var arr = line.split(',');
  var properties = {};
  var geometry = {
    "type": "Point"
  }
  var feature = {
    "type": "Feature",
    "id": [Math.random()].join('')
  };
  if(arr){
    if(!arr[1] || !arr[0] || (arr[1] == '31' && arr[0] == '123')){
      return;
    }
    geometry.coordinates = [parseFloat(arr[1]), parseFloat(arr[0])];
    properties.ndc = !(arr[2] == 'f');
    properties.rdc = !(arr[3] == 'f');
    properties.wh = !(arr[4] == 'f');
    properties.province = arr[5];
    properties.town = arr[6];
    properties.county = arr[7];

    feature.geometry = geometry;
    feature.properties = properties;
  }

  response.context.features.push(feature);
})

lines.on('end', function(){
  response.context.features.splice(0,1);
  fs.writeFile('./result.json',JSON.stringify(response));
});

