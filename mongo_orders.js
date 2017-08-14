var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var readline = require('linebyline');

var url = 'mongodb://localhost:27017/shalldon'

var lines = readline('./files/order_data.csv');
var items = [];
lines.on('line', function(line, lineCount, byteCount) {
  var arr = line.split(',');
  var item = {};

  if(arr){
    item.cbm = parseFloat(arr[0])? parseFloat(arr[0]) : null;
    item.kg = parseFloat(arr[1])? parseFloat(arr[1]) : null;
    item.tu = parseFloat(arr[2])? parseFloat(arr[2]) : null;
    item.orders = parseFloat(arr[3])? parseFloat(arr[3]) : null;
    item.origin_province = arr[4];
    item.origin_town = arr[5];
    item.origin_county = arr[6];
    item.origin_latitude = parseFloat(arr[7])? parseFloat(arr[7]) : null;
    item.origin_longitude = parseFloat(arr[8])? parseFloat(arr[8]) : null;
    item.destination_province = arr[9];
    item.destination_town = arr[10];
    item.destination_county = arr[11];
    item.destination_latitude = parseFloat(arr[12])? parseFloat(arr[12]) : null;
    item.destination_longitude = parseFloat(arr[13])? parseFloat(arr[13]) : null;
  }

  items.push(item);
})

lines.on('end', function(){
  MongoClient.connect(url, function(err, db){
    items.splice(0,1);

    var col_orders = db.collection('orders');
    if(col_orders){
      col_orders.drop();
    }
    db.createCollection('orders');
    col_orders = db.collection('orders');
    col_orders.insertMany(items);
    db.close();
  });
});

