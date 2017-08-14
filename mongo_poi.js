var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var readline = require('linebyline');

var url = 'mongodb://localhost:27017/shalldon';
var items = [];

var lines = readline('./files/shipping_points.csv');
lines.on('line', function(line, lineCount, byteCount) {
  var arr = line.split(',');
  var item = {};
  if(arr){
    item.latitude = parseFloat(arr[0])? parseFloat(arr[0]) : null;
    item.longitude = parseFloat(arr[1])? parseFloat(arr[1]) : null;
    item.ndc = !(arr[2] == 'f');
    item.rdc = !(arr[3] == 'f');
    item.wh = !(arr[4] == 'f');
    item.province = arr[5];
    item.town = arr[6];
    item.county = arr[7];

    items.push(item);
  }
})

lines.on('end', function(){
  MongoClient.connect(url, function(err, db){
    items.splice(0,1);
    var col_pois = db.collection('pois');
    if(col_pois){
      col_pois.drop();
    }
    db.createCollection('pois');
    col_pois = db.collection('pois');
    col_pois.insertMany(items);
    db.close();
  });
});
