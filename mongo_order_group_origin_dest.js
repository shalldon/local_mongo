var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var _ = require('underscore')

var sourceCol = 'orders_best_seller';
var targetCol = 'orders_best_seller_origin_dest';
// var targetCol = 'orders_best_seller_17_04_01';

var url = 'mongodb://localhost:27017/shalldon';
MongoClient.connect(url, function(err, db) {

  var col_orders_best_seller = db.collection(sourceCol);

  var orders_best_seller_origin_dest = db.collection(targetCol);
  if (orders_best_seller_origin_dest) {
    orders_best_seller_origin_dest.drop();
  }
  db.createCollection(targetCol);
  orders_best_seller_origin_dest = db.collection(targetCol);

  var startDate = new Date("2017-04-10")
  var endDate = new Date("2017-04-11")
  var content = [{
    $match: {
      $or: [{
        origin_latitude: {
          $ne: null
        }
      }, {
        origin_longitude: {
          $ne: null
        }
      }, {
        destination_latitude: {
          $ne: null
        }
      }, {
        destination_longitude: {
          $ne: null
        }
      }],
      pickup_sla: {$gte: startDate, $lte: endDate}
    }
  }, {
    $group: {
      _id: {
        origin_latitude: "$origin_latitude",
        origin_longitude: "$origin_longitude",
        destination_latitude: "$destination_latitude",
        destination_longitude: "$destination_longitude",
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
  }]

  var curcor = col_orders_best_seller.aggregate(content, {
    'allowDiskUse': true
  });


  curcor.toArray(function(err, items) {
    console.log(new Date())
    var arr = [];
    _.each(items, function(item){
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
        destination_county: item._id.destination_county,
        origin_latitude: item._id.origin_latitude,
        origin_longitude: item._id.origin_longitude,
        destination_latitude: item._id.destination_latitude,
        destination_longitude: item._id.destination_longitude
      }
      arr.push(obj);
    });
    console.log(new Date())
    console.log(arr.length)

    var len = arr.length;
    var start = 0;
    var end = 500;
    while(start<len){
      if(end > len){
        end = len;
      }

      (function(start,end){
        setTimeout(function(){
        var subArr = arr.slice(start, end);
        orders_best_seller_origin_dest.insertMany(subArr)
        console.log(start,end)
      },0)
      })(start,end)
      start = end;
      end = start + 500
    }

    // db.close()
  })

})