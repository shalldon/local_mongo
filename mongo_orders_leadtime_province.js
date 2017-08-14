var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon'

MongoClient.connect(url, {
    connectTimeoutMS: 60000,
    socketTimeoutMS: 60000
  },
  function(err, db) {
    var mapOrders = function() {
      var key = {
        destination_province: this.destination_province,
        destination_town: this.destination_town,
        destination_county: this.destination_county
      };

      var actualLeadtime = this.actual_delivery - this.actual_pickup;
      var planedLeadtime = this.delivery_sla - this.pickup_sla;

      var value = {
        orders: 1,
        delayedOrders: (actualLeadtime > planedLeadtime) ? 1 : 0,
        exceptionOrders: (actualLeadtime/planedLeadtime < 0.2) ? 1 : 0,
        cargoDemageOrders: (this.cargo_damage == 't') ? 1 : 0,
        cargoLossOrders: (this.cargo_loss == 't') ? 1 : 0
      }
      emit(key, value);
    }

    var reduceOrders = function(key, values) {
      var recudeValue = {
        orders: 0,
        delayedOrders: 0,
        exceptionOrders: 0,
        cargoDemageOrders: 0,
        cargoLossOrders: 0
      }

      values.forEach(function(value, i) {
        recudeValue.orders += value.orders;
        recudeValue.delayedOrders += value.delayedOrders;
        recudeValue.exceptionOrders += value.exceptionOrders,
        recudeValue.cargoDemageOrders += value.cargoDemageOrders,
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
            orders: item.value.orders,
            delayedOrders: item.value.delayedOrders,
            exceptionOrders: item.value.exceptionOrders,
            cargoDemageOrders: item.value.cargoDemageOrders,
            cargoLossOrders: item.value.cargoLossOrders
          }
        })
      }

      var col_orders_leadtime = db.collection('orders_leadtime');
      if (col_orders_leadtime) {
        col_orders_leadtime.drop();
      }
      db.createCollection('orders_leadtime');
      col_orders_leadtime = db.collection('orders_leadtime');
      col_orders_leadtime.insertMany(items);
      db.close();
    }

    db.collection('orders_best_seller').mapReduce(
      mapOrders,
      reduceOrders, {
        query: {
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
        },
        out: {
          inline: 1
        }
      },
      mapReduceCallback
    )
  });