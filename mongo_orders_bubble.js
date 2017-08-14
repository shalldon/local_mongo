var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var _ = require('underscore')

var sourceCol = 'orders_best_seller';
var targetCol = 'orders_best_seller_bubble';

var url = 'mongodb://localhost:27017/shalldon';

MongoClient.connect(url, function(err, db) {

  var col_orders_best_seller = db.collection(sourceCol);

  var orders_best_seller_bubble = db.collection(targetCol);
  if (orders_best_seller_bubble) {
    orders_best_seller_bubble.drop();
  }
  db.createCollection(targetCol);
  orders_best_seller_bubble = db.collection(targetCol);

  var aggregateOptions = [{
    $group: {
      _id: {
        origin_province: "$origin_province",
        origin_town: "$origin_town",
        origin_county: "$origin_county",
        destination_province: "$destination_province",
        destination_town: "$destination_town",
        destination_county: "$destination_county"
      },
      orders: {
        $sum: 1
      },
      cbm: {
        $sum: "$cbm"
      },
      kg: {
        $sum: "$kg"
      },
      tu: {
        $sum: "$tu"
      }
    }
  }];

  var curcor = col_orders_best_seller.aggregate(aggregateOptions, {
    'allowDiskUse': true
  });

  curcor.toArray(function(err, items) {
    var arr = [];
    _.each(items, function(item) {
      var obj = {
        orders: item.orders,
        cbm: item.cbm,
        kg: item.kg,
        tu: item.tu,
        origin_province: item._id.origin_province,
        origin_town: item._id.origin_town,
        origin_county: item._id.origin_county,
        destination_province: item._id.destination_province,
        destination_town: item._id.destination_town,
        destination_county: item._id.destination_county
      }
      arr.push(obj);
    });

    console.log(arr.length);

    var len = arr.length;
    var start = 0;
    var end = 500;
    while (start < len) {
      if (end > len) {
        end = len;
      }

      (function(start, end) {
        setTimeout(function() {
          var subArr = arr.slice(start, end);
          orders_best_seller_bubble.insertMany(subArr)
          console.log(start, end)
        }, 0)
      })(start, end)
      start = end;
      end = start + 500
    }

    setTimeout(function() {
      db.close();
    });
  });


});