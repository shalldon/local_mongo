var MongoClient = require('mongodb').MongoClient;
var url = 'mongodb://localhost:27017/shalldon';
var _ = require('underscore');
var fs = require('fs');


var fileurl = "./output/xy.json";
fs.readFile(fileurl, function(err, str) {

  var json = JSON.parse(str);

  var city = 'city_10';
  var city = 'city_2'

  MongoClient.connect(url, function(err, db) {
    var col_pois = db.collection(city);

    col_pois.find({}), toAttay(function(err, items) {
      _.each(items, function(item) {

      })
    })
  })


})