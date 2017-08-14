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


var lines = readline('./files/order_data.csv');
lines.on('line', function(line, lineCount, byteCount) {
  var arr = line.split(',');
  var properties = {};
  var geometry = {
    "type": "Point",
    "coordinates": []
  }
  var feature = {
    "type": "Feature",
    "id": [Math.random()].join('')
  };
  if(arr){
    // geometry.coordinates = [parseFloat(arr[1]), parseFloat(arr[0])];
    properties.cbm = parseFloat(arr[0]);
    properties.kg = parseFloat(arr[1]);
    properties.tu = parseInt(arr[2]);
    properties.orders = parseInt(arr[3]);
    properties.origin_province = arr[4];
    properties.origin_town = arr[5];
    properties.origin_county = arr[6];
    properties.origin_latitude = parseFloat(arr[7]);
    properties.origin_longitude = parseFloat(arr[8]);
    properties.destination_province = arr[9];
    properties.destination_town = arr[10];
    properties.destination_county = arr[11];
    properties.destination_latitude = parseFloat(arr[12]);
    properties.destination_longitude = parseFloat(arr[13]);


    feature.geometry = geometry;
    feature.properties = properties;
  }

  response.context.features.push(feature);
})

lines.on('end', function(){
  response.context.features.splice(0,1);
  fs.writeFile('./result.json',JSON.stringify(response));
});

