var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var readline = require('linebyline');
var _ = require('underscore');

var url = 'mongodb://localhost:27017/shalldon';
var items = [];
var filePath = './files/select_from_public_tmap_geometry_where_map_level_0_201704131749.json';
var collectionName = 'province_2'

fs.readFile(filePath, function(err, data){
  var json = JSON.parse(data);
  MongoClient.connect(url, function(err, db){
    var col_province = db.collection(collectionName);

    _.each(json.query, function(item){
      col_province.findOneAndUpdate({
        name: item.province
      },{
          $set : {
            longitude_1: parseFloat(item.longitude),
            latitude_1: parseFloat(item.latitude),
            longitude_2: parseFloat(item.extra_longitude),
            latitude_2: parseFloat(item.extra_latitude)
          }
      })
    });

    db.close();
  })
})