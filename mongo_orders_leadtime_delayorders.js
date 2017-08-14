var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon'

MongoClient.connect(url, {
  connectTimeoutMS: 60000,
  socketTimeoutMS: 60000
}, function(err, db) {

  var col_orders_delayed = db.collection('orders_delayed');
  if (col_orders_delayed) {
    col_orders_delayed.drop();
  }
  db.createCollection('orders_delayed');
  col_orders_delayed = db.collection('orders_delayed');

  var curcor = db.collection('orders_best_seller').find({
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
  });

  var arr = [];
  var i = 0;

  curcor.forEach(function(item) {
    process.nextTick(function() {
      var actualLeadtime = item.actual_delivery - item.actual_pickup;
      var planedLeadtime = item.delivery_sla - item.pickup_sla;

      if (actualLeadtime > planedLeadtime) {
        var delay_rate = (actualLeadtime - planedLeadtime)/planedLeadtime;
        item.delay_rate = delay_rate;
        col_orders_delayed.insert(item);
        i++
        if(i%1000 === 0){
          console.log(i)
        }
      }
    })
  },function(){
    console.log('finished')
    db.close();
  })

  // var insertFun = function() {
  //   curcor.next(function(err, item) {
  //     if(err){
  //       console.log(err)
  //     }
  //     var actualLeadtime = item.actual_delivery - item.actual_pickup;
  //     var planedLeadtime = item.delivery_sla - item.pickup_sla;
  //     if (actualLeadtime > planedLeadtime) {
  //       arr.push(item);

  //       if (arr.length >= 500) {
  //         i += arr.length;
  //         console.log(i)
  //         col_orders_delayed.insert(arr);
  //         arr = [];
  //       }
  //     }
  //     if (curcor.hasNext()) {
  //       process.nextTick(insertFun);
  //     }else{
  //       i += arr.length;
  //       console.log(i,'   end')
  //       col_orders_delayed.insert(arr);
  //       arr = [];
  //     }
  //   })
  // }
  // if (curcor.hasNext()) {
  //   process.nextTick(insertFun);
  // }


  // var arr = []
  // curcor.toArray(function(err, items) {
  //   console.log(items[0])
  //   _.each(items, function(item) {
  //     var actualLeadtime = item.actual_delivery - item.actual_pickup;
  //     var planedLeadtime = item.delivery_sla - item.pickup_sla;

  //     if (actualLeadtime > planedLeadtime) {
  //       arr.push(item);
  //     }
  //   });

  // })
});