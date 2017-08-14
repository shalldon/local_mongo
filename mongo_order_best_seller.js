var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var readline = require('linebyline');

var url = 'mongodb://localhost:27017/shalldon';
var fileUrl = './files/order/bs_map_data.csv';



MongoClient.connect(url, function(err, db){

  var col_orders_best_seller = db.collection('orders_best_seller');
  if(col_orders_best_seller){
    col_orders_best_seller.drop();
  }
  db.createCollection('orders_best_seller');
  col_orders_best_seller = db.collection('orders_best_seller');
  // col_orders_best_seller.insertMany(items);
  var lines = readline(fileUrl);
  lines.on('line', function(line, lineCount, byteCount){
    if(lineCount == 1) return;
    var arr = line.split(',');
    var item = {};

    if(arr){
      item.cbm = parseFloat(arr[0])? parseFloat(arr[0]) : null;
      item.kg = parseFloat(arr[1])? parseFloat(arr[1]) : null;
      item.tu = parseFloat(arr[2])? parseFloat(arr[2]) : null;
      // item.orders = parseFloat(arr[3])? parseFloat(arr[3]) : null;
      item.origin_province = arr[3];
      item.origin_town = arr[4];
      item.origin_county = arr[5];
      item.origin_latitude = parseFloat(arr[6])? parseFloat(arr[6]) : null;
      item.origin_longitude = parseFloat(arr[7])? parseFloat(arr[7]) : null;
      item.destination_province = arr[8];
      item.destination_town = arr[9];
      item.destination_county = arr[10];
      item.destination_latitude = parseFloat(arr[11])? parseFloat(arr[11]) : null;
      item.destination_longitude = parseFloat(arr[12])? parseFloat(arr[12]) : null;
      item.pickup_source = arr[13];
      item.pickup_source_name = arr[14];
      item.delivery_source = arr[15];
      item.delivery_source_name = arr[16];
      item.cargo_damage = arr[17];
      item.cargo_loss = arr[18];
      item.pickup_sla = arr[19] ? new Date(arr[19]) : null;
      item.actual_pickup = Date.parse(arr[20]) ? new Date(arr[20]) : null;
      item.delivery_sla = Date.parse(arr[21]) ? new Date(arr[21]) : null;
      item.actual_delivery = Date.parse(arr[22]) ? new Date(arr[22]) : null;

      col_orders_best_seller.insertOne(item);
    }
  })

  lines.on('end', function(){
    db.close();
  })
});


