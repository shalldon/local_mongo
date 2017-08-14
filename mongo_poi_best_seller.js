var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var readline = require('linebyline');

var url = 'mongodb://localhost:27017/shalldon';
var items = [];

var lines = readline('./files/poi/bs_mastterdata.csv');
lines.on('line', function(line, lineCount, byteCount) {
  if(lineCount == 1)return;
  var arr = line.split(',');
  var item = {};
  if(arr){
    item.latitude = parseFloat(arr[0])? parseFloat(arr[0]) : null;
    item.longitude = parseFloat(arr[1])? parseFloat(arr[1]) : null;
    item.ndc = !(arr[2] == 'f');
    item.rdc = !(arr[3] == 'f');
    item.wh = !(arr[4] == 'f');
    item.shop = !(arr[5] == 'f');
    item.province = arr[6];
    item.town = arr[7];
    item.county = arr[8];

    items.push(item);
  }
})

lines.on('end', function(){
  MongoClient.connect(url, function(err, db){
    var col_pois_best_seller = db.collection('pois_best_seller');
    if(col_pois_best_seller){
      col_pois_best_seller.drop();
    }
    db.createCollection('pois_best_seller');
    col_pois_best_seller = db.collection('pois_best_seller');
    // col_pois_best_seller.insertMany(items);

    var len = items.length;
    var start = 0;
    var end = 500;
    while(start<len){
      if(end > len){
        end = len;
      }

      (function(start,end){
        setTimeout(function(){
        var subArr = items.slice(start, end);
        col_pois_best_seller.insertMany(subArr)
        console.log(start,end)
      },0)
      })(start,end)
      start = end;
      end = start + 500
    }
    setTimeout(function(){
      db.close();
    },0)
  });
});
