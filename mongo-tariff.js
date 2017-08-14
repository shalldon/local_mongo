var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var _ = require('underscore');

var url = 'mongodb://localhost:27017/shalldon'

var fileUrl = './files/tariff/province_10%.json';
// var fileUrl = './files/tariff/province_2%.json';
// var fileUrl = './files/tariff/city_10%.json';
// var fileUrl = './files/tariff/city_2%.json';

var fileList = [{
  fileUrl: './files/tariff/province_10%.json',
  collection: 'province_10'
}, {
  fileUrl: './files/tariff/province_2%.json',
  collection: 'province_2'
}, {
  fileUrl: './files/tariff/city_10%.json',
  collection: 'city_10'
}, {
  fileUrl: './files/tariff/city_2%.json',
  collection: 'city_2'
}]

var file = fileList[2];

var insertData = function(db, col, arr) {
  var len = arr.length;
  var start = 0;
  var end = 20;
  while (start < len) {
    if (end > len) {
      end = len;
    }

    (function(start, end) {
      setTimeout(function() {
        var subArr = arr.slice(start, end);
        col.insertMany(subArr)
        console.log(start, end)
      }, 0)
    })(start, end)
    start = end;
    end = start + 20
  }
  setTimeout(function(){
    db.close()
  },0);
}

fs.readFile(file.fileUrl, 'utf-8', function(err, data){
  var json = JSON.parse(data);
  var items = _.map(json.features, function(feature){
    var obj = {
      "geometry_type": feature.geometry.type,
      "coordinates": feature.geometry.coordinates,
      "name": feature.properties.name,
      "pid": feature.properties.pid,
      "transliterate_name": feature.properties.transliterate_name,
      "alternate_name": feature.properties.alternate_name,
      "transliterate_alternate_name": feature.properties.transliterate_alternate_name,
      "language_code": feature.properties.language_code,
      "alternate_language_code": feature.properties.alternate_language_code,
      "government_code": feature.properties.government_code
    }
    if(typeof feature.properties.province_code !== "undefined"){
      obj.province_code = feature.properties.province_code;
    }

    return obj;
  });

  // fs.writeFile('./files/test_view.json', JSON.stringify(items))

  MongoClient.connect(url, function(err, db) {
    var col = db.collection(file.collection);
    if (col) {
      col.drop();
    }
    db.createCollection(file.collection);
    col = db.collection(file.collection);
    insertData(db, col, items);
    // col.insertMany(items);
    // db.close();
  })
})

