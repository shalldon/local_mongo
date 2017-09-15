var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var readline = require('linebyline');

var url = 'mongodb://localhost:27017/shalldon';
var items = [];

var lines = readline('./files/31JUL_warehouses.csv');

lines.on('line', function(line, lineCount, byteCount) {
  if(lineCount == 1)return;
  var arr = line.split(',');
  var item = {};
  if(arr){
    item.external_id = arr[0];
    item.province = arr[1];
    item.town = arr[2];
    item.county = arr[3];
    item.zipcode = arr[4];
    item.postcode = arr[5];
    item.address = arr[6];
    item.contact_name = arr[7];
    item.contact_mobile = arr[8];
    item.latitude = parseFloat(arr[9])? parseFloat(arr[9]) : null;
    item.longitude = parseFloat(arr[10])? parseFloat(arr[10]) : null;

    // var leftStr = arr.slice(12);
    // var reg = /^\{([\w\w]*)\}$/;

    // console.log(arr)

    // reg.exec(leftStr)

    // item.available_vendors = reg.exec(leftStr);
    // item.assigned_vendors = reg.exec(leftStr);

    items.push(item);
    // console.log(item)
  }
})

lines.on('end', function(){
  MongoClient.connect(url, function(err, db){
    var col_warehouse = db.collection('warehouse');
    if(col_warehouse){
      col_warehouse.drop();
    }
    db.createCollection('warehouse');
    col_warehouse = db.collection('warehouse');
    col_warehouse.insertMany(items);
    db.close();
  });
  console.log()
});

