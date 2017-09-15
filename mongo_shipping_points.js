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
    if (lineCount % 1000 === 0) {
      console.log(lineCount);
    }
    var arr = line.split(',');
    var item = {};
    if (arr) {
      item.postcode = arr[0];
      item.external_id = arr[1];
      item.province = arr[2]
      item.town = arr[3]
      item.county = arr[4]
      item.zipcode = arr[5]
      if (arr[6].startsWith('"')) {
        if (arr.length > 15) {
          var i = arr.length - 1;
          var addressArr = arr.slice(6, i - 7);

          var addressResult = /\"?([\w\W]*)\"?/.exec(addressArr.join(''));
          if (addressResult && addressResult.length > 1) {
            item.address = addressResult[1];
          } else {
            item.address = "";
          }
          item.name = arr[i - 8];
          item.mobile = arr[i - 7];
          item.locations = (arr[i - 6] == "{}") ? null : arr[i - 6];
          item.ndc = !(arr[i - 5] == 'f');
          item.rdc = !(arr[i - 4] == 'f');
          item.latitude = parseFloat(arr[i - 3]) ? parseFloat(arr[i - 3]) : null;
          item.longitude = parseFloat(arr[i - 2]) ? parseFloat(arr[i - 2]) : null;
          item.wh_id = arr[i - 1];
          item.case = arr[i];
        } else {
          item.mobile = arr[8]
          item.locations = (arr[9] == "{}") ? null : arr[9];
          item.ndc = !(arr[10] == 'f');
          item.rdc = !(arr[11] == 'f');
          item.latitude = parseFloat(arr[12]) ? parseFloat(arr[12]) : null;
          item.longitude = parseFloat(arr[13]) ? parseFloat(arr[13]) : null;
          item.wh_id = arr[14];
          item.case = arr[15];
        }
      } else if (arr[7].startsWith('"')) {
        item.address = arr[6];
        if (arr.length > 15) {
          var i = arr.length - 1;
          var nameArr = arr.slice(7, i - 7);

          var nameResult = /\"?([\w\W]*)\"?/.exec(nameArr.join(','));
          if (nameResult && nameResult.length > 1) {
            item.name = nameResult[1];
          } else {
            item.name = "";
          }
          item.mobile = arr[i - 7];
          item.locations = (arr[i - 6] == "{}") ? null : arr[i - 6];
          item.ndc = !(arr[i - 5] == 'f');
          item.rdc = !(arr[i - 4] == 'f');
          item.latitude = parseFloat(arr[i - 3]) ? parseFloat(arr[i - 3]) : null;
          item.longitude = parseFloat(arr[i - 2]) ? parseFloat(arr[i - 2]) : null;
          item.wh_id = arr[i - 1];
          item.case = arr[i];
        } else {
          item.mobile = arr[8]
          item.locations = (arr[9] == "{}") ? null : arr[9];
          item.ndc = !(arr[10] == 'f');
          item.rdc = !(arr[11] == 'f');
          item.latitude = parseFloat(arr[12]) ? parseFloat(arr[12]) : null;
          item.longitude = parseFloat(arr[13]) ? parseFloat(arr[13]) : null;
          item.wh_id = arr[14];
          item.case = arr[15];
        }
      } else {
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
      }

      process.nextTick(function() {
        col_shipping.insert(item);
      })
    }
  }, function() {
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