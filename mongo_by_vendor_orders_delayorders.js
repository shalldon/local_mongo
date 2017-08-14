var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon'

MongoClient.connect(url, {
  connectTimeoutMS: 60000,
  socketTimeoutMS: 60000
}, function(err, db) {

  var by_vendor_orders_delayed = db.collection('by_vendor_orders_delayed');
  if (by_vendor_orders_delayed) {
    by_vendor_orders_delayed.drop();
  }
  db.createCollection('by_vendor_orders_delayed');
  by_vendor_orders_delayed = db.collection('by_vendor_orders_delayed');

  var curcor = db.collection('orders_detail_24May').find({});

  var arr = [];
  var i = 0;

  curcor.forEach(function(item) {
    process.nextTick(function() {
      var actualLeadtime = item.actual_delivery - item.actual_pickup;
      var planedLeadtime = item.delivery_sla - item.pickup_sla;

      if (actualLeadtime > planedLeadtime) {
        var delay_rate = (actualLeadtime - planedLeadtime)/planedLeadtime;
        item.delay_rate = delay_rate;
        item.vendor_code = item.vendor_short_code;
        delete item.vendor_short_code;
        by_vendor_orders_delayed.insert(item);
        i++
        if(i%1000 === 0){
          console.log(i)
        }
      }
    });
  },function(){
    console.log('finished')
    db.close();
  })
});