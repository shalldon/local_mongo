var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectID;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon'

MongoClient.connect(url, {
  connectTimeoutMS: 60000,
  socketTimeoutMS: 60000
}, function(err, db) {

  var col_orders = db.collection('orders_best_seller');
  var col_shipping = db.collection('shipping_points');
  var count = 0

  col_orders.find({
    pickup_sla: {
      $ne: null
    },
    delivery_sla: {
      $ne: null
    },
    $or: [{
      origin_shipping_point: null
    }, {
      destination_shipping_point: null
    }]
  }).forEach(function(item) {

    process.nextTick(function() {
      updateNearestShippingPoints(0, item, col_shipping, col_orders)
    });
    process.nextTick(function() {
      updateNearestShippingPoints(1, item, col_shipping, col_orders)
    });
    count++;
    if (count % 1000 == 0) {
      console.log(count)
    }

  }, function(err) {

  })
});

function updateNearestShippingPoints(shipType, item, col_shipping, col_orders) {
  var fields = ['province', 'town', 'county'];
  var searchOptions = {
    latitude: {
      $ne: null
    },
    longitude: {
      $ne: null
    }
  };
  _.each(fields, function(name) {
    var fieldName = (shipType == 0) ? ['origin', name].join('_') : ['destination', name].join('_');

    if (item[fieldName]) {
      searchOptions[name] = item[fieldName];
    }
  });
  col_shipping.find(searchOptions).toArray(function(err, shippingPoints) {
    if (err) {
      console.log(err);
      return;
    }
    if (!shippingPoints || !shippingPoints.length) {
      return;
    }
    var nearstShippingPoint = {
      shippingPoint: null,
      distance: null
    }
    _.each(shippingPoints, function(shippingPoint) {
      var item_latitude = (shipType == 0) ? ['origin', 'latitude'].join('_') : ['destination', 'latitude'].join('_');
      var item_longitude = (shipType == 0) ? ['origin', 'longitude'].join('_') : ['destination', 'longitude'].join('_');
      var distance = Math.pow(shippingPoint.latitude - item[item_latitude], 2) + Math.pow(shippingPoint.longitude - item[item_longitude], 2);

      if (_.isNull(nearstShippingPoint.shippingPoint) || nearstShippingPoint.distance > distance) {
        nearstShippingPoint.shippingPoint = shippingPoint;
        nearstShippingPoint.distance = distance;
      }
    });

    var setField = (shipType == 0) ? 'origin_shipping_point' : 'destination_shipping_point';
    var setOptions = {};
    setOptions[setField] = nearstShippingPoint.shippingPoint.external_id;
    col_orders.updateOne({
      _id: ObjectID(item._id)
    }, {
      $set: setOptions
    })
  })
}