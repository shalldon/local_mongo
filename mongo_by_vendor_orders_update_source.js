var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon';
var fs = require('fs')

MongoClient.connect(url, {
  connectTimeoutMS: 120000,
  socketTimeoutMS: 120000
}, function(err, db) {

  var col_orders = db.collection('orders_detail_24May');

  var keys = {
    destination_province: 1,
    destination_town: 1,
    destination_county: 1,
    vendor_short_code: 1,
    vendor_name: 1
  };

  var condition = {};

  var init = {
    orders: 0,
    pickup: {},
    delivery: {}
  }

  var reduce = function(curr, result) {
    if (!result.pickup[curr.pickup_source_name]) {
      result.pickup[curr.pickup_source_name] = 1;
    } else {
      result.pickup[curr.pickup_source_name] += 1;
    }
    if (!result.delivery[curr.delivery_source_name]) {
      result.delivery[curr.delivery_source_name] = 1;
    } else {
      result.delivery[curr.delivery_source_name] += 1;
    }
  }

  var finalize = function(result) {

  }

  var callback = function(err, results) {
    if (err) {
      console.log(err)
    }
    var by_vendor_orders_update_source = db.collection('by_vendor_orders_update_source');
    if (by_vendor_orders_update_source) {
      by_vendor_orders_update_source.drop();
    }
    db.createCollection('by_vendor_orders_update_source');
    by_vendor_orders_update_source = db.collection('by_vendor_orders_update_source');
    _.each(results, function(item){
      item.vendor_code = item.vendor_short_code;
      delete item.vendor_short_code;
    })

    // fs.writeFile("./output/update_source.json", JSON.stringify(results))
    insertArrays(results, by_vendor_orders_update_source, 20);
    process.nextTick(function() {
      db.close();
    })
  }

  col_orders.group(keys, condition, init, reduce, finalize, callback);
});

function insertArrays(items, collection, step) {
  if (!step) {
    step = 500;
  }
  var len = items.length;
  var start = 0;
  var end = start + step;
  while (start < len) {
    if (end > len) {
      end = len;
    }

    (function(start, end) {
      process.nextTick(function() {
        var subItems = items.slice(start, end);
        collection.insertMany(subItems)
        console.log(start, end)
      });
    })(start, end)
    start = end;
    end = start + step;
  }
}