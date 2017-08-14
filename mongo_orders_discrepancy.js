var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon'
var fs = require('fs');

MongoClient.connect(url, {
    connectTimeoutMS: 480000,
    socketTimeoutMS: 480000
  },
  function(err, db) {

    var condition = {
      pickup_sla: {
        $ne: null
      },
      actual_pickup: {
        $ne: null
      },
      delivery_sla: {
        $ne: null
      },
      actual_delivery: {
        $ne: null
      }
    };

    var keys = {
      destination_province: 1,
      destination_town: 1,
      destination_county: 1
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
          cargoDemageOrders: 0
        }
      }

      result.orders += 1;
      result.byDate[dateStr].orders += 1;

      if(curr.cargo_loss == 't'){
        result.byDate[dateStr].cargoLossOrders += 1;
      }
      if(curr.cargo_damage == 't'){
        result.byDate[dateStr].cargoDemageOrders += 1;
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

      var orders_discrepancy = db.collection('orders_discrepancy');
      if (orders_discrepancy) {
        orders_discrepancy.drop();
      }
      db.createCollection('orders_discrepancy');
      orders_discrepancy = db.collection('orders_discrepancy');

      _.each(items, function(item, i){
        var itemsByPlace = _.map(item.byDate, function(value, date){
          return {
            province: item.destination_province,
            town: item.destination_town,
            county: item.destination_county,
            year: value.year,
            month: value.month,
            date: value.date,
            orders: value.orders,
            cargoLossOrders: value.cargoLossOrders,
            cargoDemageOrders: value.cargoDemageOrders
          }
        });
        insertArrays(itemsByPlace, orders_discrepancy);


      });
      process.nextTick(function(){
        db.close();
      });
    }

    db.collection('orders_best_seller').group(
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