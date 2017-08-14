var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon';

MongoClient.connect(url, {
  connectTimeoutMS: 60000,
  socketTimeoutMS: 60000
}, function(err, db) {

  var col_orders_exception = db.collection('orders_exception');
  if (col_orders_exception) {
    col_orders_exception.drop();
  }
  db.createCollection('orders_exception');
  col_orders_exception = db.collection('orders_exception');

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

  var i = 0;

  curcor.forEach(function(item){
    process.nextTick(function(){
      var actualLeadtime = item.actual_delivery - item.actual_pickup;
      var planedLeadtime = item.delivery_sla - item.pickup_sla;

      var rate = actualLeadtime/planedLeadtime;
      if (rate <= 0.2) {
        item.rate = rate;
        col_orders_exception.insert(item);
        i++
        if(i%1000 === 0){
          console.log(i)
        }
      }

    });
  },function(){
    process.nextTick(function(){
      console.log('finished')
      db.close();
    });
  })
})