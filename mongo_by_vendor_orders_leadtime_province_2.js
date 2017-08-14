var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon'

MongoClient.connect(url, {
    connectTimeoutMS: 480000,
    socketTimeoutMS: 480000
  },
  function(err, db) {
    // var mapOrders = function() {
    //   var key = {
    //     origin_province: this.origin_province,
    //     origin_town: this.origin_town,
    //     origin_county: this.origin_county,
    //     destination_province: this.destination_province,
    //     destination_town: this.destination_town,
    //     destination_county: this.destination_county,
    //     vendor_code: this.vendor_short_code,
    //     vendor_name: this.vendor_name
    //   };

    //   var actualLeadtime = this.actual_delivery - this.actual_pickup;
    //   var planedLeadtime = this.delivery_sla - this.pickup_sla;

    //   var value = {
    //     orders: 1,
    //     delayedOrders: (actualLeadtime > planedLeadtime) ? 1 : 0,
    //     onTimePickupOrders: (this.actual_pickup < this.pickup_sla) ? 1 : 0,
    //     onTimeDeliveryOrders: (this.actual_delivery < this.delivery_sla) ? 1: 0,
    //     exceptionOrders: (actualLeadtime/planedLeadtime < 0.2) ? 1 : 0,
    //     cargoDamageOrders: this.cargo_damage_orders ? 1 : 0,
    //     cargoLossOrders: this.cargo_loss_orders ? 1 : 0
    //   }
    //   emit(key, value);
    // }

    // var reduceOrders = function(key, values) {
    //   var recudeValue = {
    //     orders: 0,
    //     delayedOrders: 0,
    //     onTimePickupOrders: 0,
    //     onTimeDeliveryOrders: 0,
    //     exceptionOrders: 0,
    //     cargoDamageOrders: 0,
    //     cargoLossOrders: 0
    //   };

    //   values.forEach(function(value, i) {
    //     recudeValue.orders += value.orders;
    //     recudeValue.delayedOrders += value.delayedOrders;
    //     recudeValue.onTimePickupOrders += value.onTimePickupOrders;
    //     recudeValue.onTimeDeliveryOrders += value.onTimeDeliveryOrders;
    //     recudeValue.exceptionOrders += value.exceptionOrders;
    //     recudeValue.cargoDamageOrders += value.cargoDamageOrders;
    //     recudeValue.cargoLossOrders += value.cargoLossOrders
    //   });

    //   return recudeValue;
    // }

    var condition = {};
    var keys = {
      origin_province: 1,
      origin_town: 1,
      origin_county: 1,
      destination_province: 1,
      destination_town: 1,
      destination_county: 1,
      vendor_short_code: 1,
      vendor_name: 1
    };

    var init = {
      orders: 0,
      cbm: 0,
      kg: 0,
      tu: 0,
      delayedOrders: 0,
      onTimePickupOrders: 0,
      onTimeDeliveryOrders: 0,
      exceptionOrders: 0,
      cargoDamageOrders: 0,
      cargoLossOrders: 0
    };

    var reduce = function(curr, result){
      var actualLeadtime = curr.actual_delivery - curr.actual_pickup;
      var planedLeadtime = curr.delivery_sla - curr.pickup_sla;

      result.orders += 1;
      result.cbm += curr.cbm;
      result.kg += curr.kg;
      result.tu += curr.tu;
      if(actualLeadtime > planedLeadtime){
        result.delayedOrders += 1;
      }
      if(curr.actual_pickup < curr.pickup_sla){
        result.onTimePickupOrders += 1;
      }
      if(curr.actual_delivery < curr.delivery_sla){
        result.onTimeDeliveryOrders += 1;
      }
      if(actualLeadtime < planedLeadtime * 0.2){
        result.exceptionOrders += 1;
      }
      if(curr.cargo_damage_orders){
        result.cargoDamageOrders += 1;
      }
      if(curr.cargo_loss_orders){
        result.cargoLossOrders += 1;
      }
    }

    var finalize = function(result) {

    }

    var callback = function(err, arr) {
      if (err) {
        console.log(err)
        return;
      }


      var items = _.map(arr, function(item, i) {
        return {
          pickup_province: item.origin_province,
          pickup_town: item.origin_town,
          pickup_county: item.origin_county,
          delivery_province: item.destination_province,
          delivery_town: item.destination_town,
          delivery_county: item.destination_county,
          vendor_code: item.vendor_short_code,
          vendor_name: item.vendor_name,
          orders: item.orders,
          cbm: item.cbm,
          kg: item.kg,
          tu: item.tu,
          delayedOrders: item.delayedOrders,
          onTimePickupOrders: item.onTimePickupOrders,
          onTimeDeliveryOrders: item.onTimeDeliveryOrders,
          exceptionOrders: item.exceptionOrders,
          cargoDamageOrders: item.cargoDamageOrders,
          cargoLossOrders: item.cargoLossOrders
        }
      });

      var by_vendor_orders_leadtime_2 = db.collection('by_vendor_orders_leadtime_2');
      if (by_vendor_orders_leadtime_2) {
        by_vendor_orders_leadtime_2.drop();
      }
      db.createCollection('by_vendor_orders_leadtime_2');
      by_vendor_orders_leadtime_2 = db.collection('by_vendor_orders_leadtime_2');
      // by_vendor_orders_leadtime.insertMany(items);
      insertArrays(items, by_vendor_orders_leadtime_2, db);
    }

    db.collection('orders_detail_24May').group(
      keys, condition, init, reduce, finalize, callback
    )

    // db.collection('orders_detail_24May').mapReduce(
    //   mapOrders,
    //   reduceOrders, {
    //     out: {
    //       inline: 1
    //     }
    //   },
    //   mapReduceCallback
    // )
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
