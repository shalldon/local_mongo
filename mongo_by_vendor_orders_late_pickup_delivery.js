var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon'

MongoClient.connect(url, {
  connectTimeoutMS: 60000,
  socketTimeoutMS: 60000
}, function(err, db) {

  var by_vendor_orders_late_pickup_delivery = db.collection('by_vendor_orders_late_pickup_delivery');
  if (by_vendor_orders_late_pickup_delivery) {
    by_vendor_orders_late_pickup_delivery.drop();
  }
  db.createCollection('by_vendor_orders_late_pickup_delivery');
  by_vendor_orders_late_pickup_delivery = db.collection('by_vendor_orders_late_pickup_delivery');

  var curcor = db.collection('orders_detail_24May').find({});

  var arr = [];
  var pickIndex = 0;
  var deliveryIndex = 0;

  curcor.forEach(function(item) {
    process.nextTick(function() {
      var isPickupLate = (item.actual_pickup > item.pickup_sla);
      var isDeliveryLate = (item.actual_delivery > item.delivery_sla);

      if(isPickupLate){
        pickIndex++;
        if(pickIndex%1000 === 0){
          console.log('late pickup', pickIndex)
        }
        item.latePickupDays = (item.actual_pickup - item.pickup_sla)/(1000*3600*24);
      }
      if(isDeliveryLate){
        deliveryIndex++;
        if(deliveryIndex%1000 === 0){
          console.log('late delivery', deliveryIndex)
        }
        item.lateDeliveryDays = (item.actual_delivery - item.delivery_sla)/(1000*3600*24);
      }

      if(isPickupLate || isDeliveryLate){
        item.vendor_code = item.vendor_short_code;
        delete item.vendor_short_code;

        by_vendor_orders_late_pickup_delivery.insert(item);
      }
    });
  },function(){
    console.log('finished')
    db.close();
  })
});
