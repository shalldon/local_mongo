var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon'

MongoClient.connect(url, {
    connectTimeoutMS: 240000,
    socketTimeoutMS: 240000
  },
  function(err, db) {
    var mapOrders = function() {
      var key = {
        destination_province: this.destination_province,
        destination_town: this.destination_town,
        destination_county: this.destination_county,
        vendor_code: this.vendor_short_code,
        vendor_name: this.vendor_name
      };

      var actualLeadtime = this.actual_delivery - this.actual_pickup;
      var planedLeadtime = this.delivery_sla - this.pickup_sla;

      var value = {
        orders: 1,
        delayedOrders: (actualLeadtime > planedLeadtime) ? 1 : 0,
        onTimePickupOrders: (this.actual_pickup < this.pickup_sla) ? 1 : 0,
        onTimeDeliveryOrders: (this.actual_delivery < this.delivery_sla) ? 1: 0,
        exceptionOrders: (actualLeadtime/planedLeadtime < 0.2) ? 1 : 0,
        cargoDamageOrders: this.cargo_damage_orders ? 1 : 0,
        cargoLossOrders: this.cargo_loss_orders ? 1 : 0
      }
      emit(key, value);
    }

    var reduceOrders = function(key, values) {
      var recudeValue = {
        orders: 0,
        delayedOrders: 0,
        onTimePickupOrders: 0,
        onTimeDeliveryOrders: 0,
        exceptionOrders: 0,
        cargoDamageOrders: 0,
        cargoLossOrders: 0
      };

      values.forEach(function(value, i) {
        recudeValue.orders += value.orders;
        recudeValue.delayedOrders += value.delayedOrders;
        recudeValue.onTimePickupOrders += value.onTimePickupOrders;
        recudeValue.onTimeDeliveryOrders += value.onTimeDeliveryOrders;
        recudeValue.exceptionOrders += value.exceptionOrders;
        recudeValue.cargoDamageOrders += value.cargoDamageOrders;
        recudeValue.cargoLossOrders += value.cargoLossOrders
      });

      return recudeValue;
    }

    var mapReduceCallback = function(err, arr) {
      if (err) {
        console.log(err)
        return;
      }
      var items = [];

      if (arr && arr.length) {
        items = _.map(arr, function(item, i) {
          return {
            province: item._id.destination_province,
            town: item._id.destination_town,
            county: item._id.destination_county,
            vendor_code: item._id.vendor_code,
            vendor_name: item._id.vendor_name,
            orders: item.value.orders,
            delayedOrders: item.value.delayedOrders,
            onTimePickupOrders: item.value.onTimePickupOrders,
            onTimeDeliveryOrders: item.value.onTimeDeliveryOrders,
            exceptionOrders: item.value.exceptionOrders,
            cargoDamageOrders: item.value.cargoDamageOrders,
            cargoLossOrders: item.value.cargoLossOrders
          }
        });
      }

      var by_vendor_orders_leadtime = db.collection('by_vendor_orders_leadtime');
      if (by_vendor_orders_leadtime) {
        by_vendor_orders_leadtime.drop();
      }
      db.createCollection('by_vendor_orders_leadtime');
      by_vendor_orders_leadtime = db.collection('by_vendor_orders_leadtime');
      // by_vendor_orders_leadtime.insertMany(items);
      insertArrays(items, by_vendor_orders_leadtime, db);
    }

    db.collection('orders_detail_24May').mapReduce(
      mapOrders,
      reduceOrders, {
        out: {
          inline: 1
        }
      },
      mapReduceCallback
    )
  });

function insertArrays(items, collection, db, step) {
  if(!db){
    console.log('no db parameter')
    return;
  }
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
  process.nextTick(function(){
    db.close();
  });
}
