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
    on_time_pickup_orders: 0,
    on_time_delivery_orders: 0,
    exception_orders: 0,
    cbm:0,
    kg:0,
    tu:0,
    pickup:{},
    delivery:{},
    cargo_damage_orders: 0,
    cargo_loss_orders: 0
  }

  var reduce = function(curr, result){
    var actualLeadtime = curr.actual_delivery - curr.actual_pickup;
    var planedLeadtime = curr.delivery_sla - curr.pickup_sla;

    if(curr.actual_pickup < curr.pickup_sla){
      result.on_time_pickup_orders += 1;
    }
    if(curr.actual_delivery < curr.delivery_sla){
      result.on_time_delivery_orders += 1;
    }
    if(actualLeadtime < planedLeadtime*0.2){
      result.exception_orders += 1;
    }
    result.orders += 1;
    result.cbm += curr.cbm;
    result.kg += curr.kg;
    result.tu += curr.tu;

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
    if(curr.cargo_damage_orders){
      result.cargo_damage_orders += 1
    }
    if(curr.cargo_loss_orders){
      result.cargo_loss_orders += 1
    }
  }

  var finalize = function(result) {

  }

  var callback = function(err, results) {
    if (err) {
      console.log(err)
    }
    var by_vendor_orders_grid = db.collection('by_vendor_orders_grid');
    if (by_vendor_orders_grid) {
      by_vendor_orders_grid.drop();
    }
    db.createCollection('by_vendor_orders_grid');
    by_vendor_orders_grid = db.collection('by_vendor_orders_grid');
    _.each(results, function(item){
      item.vendor_code = item.vendor_short_code;
      delete item.vendor_short_code;
    })

    // fs.writeFile("./output/update_source.json", JSON.stringify(results))
    insertArrays(results, by_vendor_orders_grid, 20);
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
