var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon'
var fs = require('fs');

MongoClient.connect(url, {
    connectTimeoutMS: 480000,
    socketTimeoutMS: 480000
  },
  function(err, db) {

    var condition = {};

    var keys = {
      destination_province: 1,
      destination_town: 1,
      destination_county: 1,
      vendor_short_code: 1,
      vendor_name: 1
    };

    var init = {
      orders: 0,
      byDate:{}
    }

    var reduce = function(curr, result) {
      var year = curr.pickup_sla.getFullYear();
      var month = curr.pickup_sla.getMonth() + 1;
      var date = curr.pickup_sla.getDate();
      var dateStr = [year, month, date].join('-');

      if(!result.byDate[dateStr]){
        result.byDate[dateStr] = {
          year: year,
          month: month,
          date: date,
          orders: 0,
          cargoLossOrders: 0,
          cargoDamageOrders: 0
        }
      }

      result.orders += 1;
      result.byDate[dateStr].orders += 1;

      if(curr.cargo_loss_orders){
        result.byDate[dateStr].cargoLossOrders += 1;
      }
      if(curr.cargo_damage_orders){
        result.byDate[dateStr].cargoDamageOrders += 1;
      }

    }

    var finalize = function(result) {

    }

    var callBack = function(err, items) {
      if(err){
        console.log(err)
      }
      console.log('finished');
      // fs.writeFile('./output/discrepancy.json', JSON.stringify(items));

      var by_vendor_orders_discrepancy = db.collection('by_vendor_orders_discrepancy');
      if (by_vendor_orders_discrepancy) {
        by_vendor_orders_discrepancy.drop();
      }
      db.createCollection('by_vendor_orders_discrepancy');
      by_vendor_orders_discrepancy = db.collection('by_vendor_orders_discrepancy');

      _.each(items, function(item, i){
        var itemsByPlace = _.map(item.byDate, function(value, date){
          return {
            province: item.destination_province,
            town: item.destination_town,
            county: item.destination_county,
            vendor_code: item.vendor_short_code,
            vendor_name: item.vendor_name,
            year: value.year,
            month: value.month,
            date: value.date,
            orders: value.orders,
            cargoLossOrders: value.cargoLossOrders,
            cargoDamageOrders: value.cargoDamageOrders
          }
        });
        insertArrays(itemsByPlace, by_vendor_orders_discrepancy);


      });
      process.nextTick(function(){
        db.close();
      });
    }

    db.collection('orders_detail_24May').group(
      keys, condition, init, reduce, finalize, callBack
    )

  });

function insertArrays(items, collection, step) {
  if(!step){
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
      process.nextTick(function(){
        var subItems = items.slice(start, end);
        collection.insertMany(subItems)
        console.log(start, end)
      });
    })(start, end)
    start = end;
    end = start + step;
  }
}
