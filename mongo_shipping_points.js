var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var readline = require('linebyline');
var url = 'mongodb://localhost:27017/shalldon';
var items = [];

MongoClient.connect(url, function(err, db) {
  var col_shipping = db.collection('shipping_points');
  if (col_shipping) {
    col_shipping.drop();
  }
  db.createCollection('shipping_points');
  col_shipping = db.collection('shipping_points');

  var lines = readline('./files/31JUL_shipping_points.csv');
  lines.on('line', function(line, lineCount, byteCount) {
    if (lineCount == 1) return;
    var arr = line.split(',');
    var item = {};
    if (arr) {
      item.postcode = arr[0];
      item.external_id = arr[1];
      item.province = arr[2]
      item.town = arr[3]
      item.county = arr[4]
      item.zipcode = arr[5]
      item.address = arr[6]
      item.name = arr[7]
      item.mobile = arr[8]
      item.locations = (arr[9] == "{}") ? null : arr[9];
      item.ndc = !(arr[10] == 'f');
      item.rdc = !(arr[11] == 'f');
      item.latitude = parseFloat(arr[12]) ? parseFloat(arr[12]) : null;
      item.longitude = parseFloat(arr[13]) ? parseFloat(arr[13]) : null;
      item.wh_id = arr[14];
      item.case = arr[15];

      process.nextTick(function(){
        col_shipping.insert(item);
      })
    }
  }, function(){
    console.log('end')
    db.close();
  })
});



// lines.on('end', function() {
//   MongoClient.connect(url, function(err, db) {
//     items.splice(0, 1);

//     col_shipping.insertMany(items);
//     db.close();
//   });
// });