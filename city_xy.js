var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var readline = require('linebyline');
var path = require('path');
var _ = require('underscore');

var fileurl = "./files/city/city_xy.txt";
var url = 'mongodb://localhost:27017/shalldon';


var outputPath = './output';
var obj = {}

// lines.on('line', function(line, lineCount, byteCount) {
//   if (lineCount == 1) return;

//   var arr = line.split(/\s+/);
//   var province = arr[0];
//   var city = arr[1];
//   var county = arr[2];
//   // console.log(province, city, county)
//   var longitude = parseFloat(arr[3]);
//   var latitude = parseFloat(arr[4]);

//   if(!obj[province]){
//     obj[province] = {}
//   }
//   if(!obj[province][city]){
//     obj[province][city] = {}
//   }
//   if(!obj[province][city][county]){
//     obj[province][city][county] = {}
//   }
//   if(province == city && city == county){
//     obj[province].longitude = longitude;
//     obj[province].latitude = latitude;
//   }else if(province != city && city == county){
//     obj[province][city].longitude = longitude;
//     obj[province][city].latitude = latitude;
//   }else{
//     obj[province][city][county].longitude = longitude;
//     obj[province][city][county].latitude = latitude;
//   }

// })

// lines.on('end', function() {
//   fs.writeFile(path.join(outputPath, 'xy.json'), JSON.stringify(obj))
// })


MongoClient.connect(url, function(err, db) {
  // var city = 'city_10';
  var city = 'city_2';
  var col_city = db.collection(city);
  var lines = readline(fileurl);
  lines.on('line', function(line, lineCount, byteCount) {
    if (lineCount == 1) return;

    var arr = line.split(/\s+/);
    var province = arr[0];
    var city = arr[1];
    var county = arr[2];
    // console.log(province, city, county)
    var longitude = parseFloat(arr[3]);
    var latitude = parseFloat(arr[4]);
    // console.log(lineCount, line)
    process.nextTick(function() {
      col_city.findOneAndUpdate({
        $or: [ {
          name: county
        }]
      }, {
        $set: {
          longitude: longitude,
          latitude: latitude
        }
      });
      console.log(line)
    })
  })

  lines.on('end', function() {
    db.close();
  })
})