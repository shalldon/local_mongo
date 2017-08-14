// var fs = require('fs');
var _ = require('underscore');

// var path = "./files/test.json"
// fs.readFile(path, 'utf-8', function(err, data){
//   var json = JSON.parse(data);
//   _.each(json.context.features,function(feature, i){
//     var coordinates = feature.geometry.coordinates;
//     if(!coordinates || !coordinates[0] || !coordinates[1]){
//       // console.log(JSON.stringify(feature))
//       console.log(i)
//     }
//     if(coordinates[0] < coordinates[1]){
//       // console.log(JSON.stringify(feature))
//       console.log(i)
//     }
//   })
// })

// var a = [
//   {
//     "province" : "北京市"
//   },
//   {
//     "province" : "天津市"
//   },
//   {
//     "province" : "上海市"
//   },
//   {
//     "province" : "重庆市"
//   },
//   {
//     "province" : "河北省"
//   },
//   {
//     "province" : "山西省"
//   },
//   {
//     "province" : "辽宁省"
//   },
//   {
//     "province" : "吉林省"
//   },
//   {
//     "province" : "黑龙江省"
//   },
//   {
//     "province" : "江苏省"
//   },
//   {
//     "province" : "浙江省"
//   },
//   {
//     "province" : "安徽省"
//   },
//   {
//     "province" : "福建省"
//   },
//   {
//     "province" : "江西省"
//   },
//   {
//     "province" : "山东省"
//   },
//   {
//     "province" : "河南省"
//   },
//   {
//     "province" : "湖北省"
//   },
//   {
//     "province" : "湖南省"
//   },
//   {
//     "province" : "广东省"
//   },
//   {
//     "province" : "海南省"
//   },
//   {
//     "province" : "四川省"
//   },
//   {
//     "province" : "贵州省"
//   },
//   {
//     "province" : "云南省"
//   },
//   {
//     "province" : "陕西省"
//   },
//   {
//     "province" : "甘肃省"
//   },
//   {
//     "province" : "青海省"
//   },
//   {
//     "province" : "西藏自治区"
//   },
//   {
//     "province" : "广西壮族自治区"
//   },
//   {
//     "province" : "内蒙古自治区"
//   },
//   {
//     "province" : "宁夏回族自治区"
//   },
//   {
//     "province" : "新疆维吾尔自治区"
//   },
//   {
//     "province" : "香港特别行政区"
//   },
//   {
//     "province" : "澳门特别行政区"
//   }
// ]
// var b = [];

// a.forEach(function(item){
//   b.push(item.province)
// })

// console.log(b)


// var arr = [];

// for (i = 0; i < 1988; i++) {
//   arr.push(1);
// }

// var len = arr.length;
// var start = 0;
// var end = 500;
// while (start < len) {

//   if (end > len) {
//     end = len;
//   }
//   var subArr = arr.slice(start, end);
//   console.log(start,end)
//   // (function(subArr) {
//   //   setTimeout(function() {
//   //     orders_best_seller_origin_dest.insertMany(subArr)
//   //   }, 0)
//   // })(subArr)
//   start = end;
//   end = start + 500
// }

// var items = [{
//   label: "2017-3",
//   year: 2017,
//   month: 3,
//   orders: 1802,
//   cargoDemageOrders: 16,
//   cargoLossOrders: 13
// },
// // {
// //   label: "2017-2",
// //   year: 2017,
// //   month: 2,
// //   orders: 1290,
// //   cargoDemageOrders: 15,
// //   cargoLossOrders: 18
// // },
// {
//   label: "2017-4",
//   year: 2017,
//   month: 4,
//   orders: 40,
//   cargoDemageOrders: 0,
//   cargoLossOrders: 0
// }, {
//   label: "2017-1",
//   year: 2017,
//   month: 1,
//   orders: 2150,
//   cargoDemageOrders: 8,
//   cargoLossOrders: 17
// }, {
//   label: "2018-6",
//   year: 2018,
//   month: 6,
//   orders: 2150,
//   cargoDemageOrders: 8,
//   cargoLossOrders: 17
// }]

// var fillItems = function(items) {
//   var filledItems = [];

//   items.sort((item1, item2) => {
//     if (item1.year !== item2.year) {
//       return item1.year - item2.year;
//     } else {
//       return item1.month - item2.month;
//     }
//   });
//   _.each(items, (item, i) => {
//     if (i == 0) {
//       filledItems.push(item);
//     }else{
//       var lastItem = filledItems[filledItems.length - 1];
//       var itemsToFilled = [];

//       while ((lastItem.year * 12 + lastItem.month + 1) < (item.year * 12 + item.month)) {
//         var newItem = {
//           year: lastItem.year,
//           month: lastItem.month + 1,
//           orders: 0,
//           cargoDemageOrders: 0,
//           cargoLossOrders: 0
//         };

//         if (newItem.month > 12) {
//           newItem.month -= 12;
//           newItem.year += 1;
//         }
//         newItem.label = [newItem.year, newItem.month].join('-');
//         itemsToFilled.push(newItem);
//         lastItem = newItem;
//       }
//       itemsToFilled.push(item);
//       // console.log(itemsToFilled)
//       filledItems = filledItems.concat(itemsToFilled);
//     }
//   });

//   return filledItems;
// };

// var filledItems = fillItems(items)
// console.log(filledItems);


