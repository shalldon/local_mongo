var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var _ = require('underscore');
var url = 'mongodb://localhost:27017/shalldon';
var fileurl = "./output/province_ziped.json";
var displayLevel = 1;
var encodeScale = 1024;
var sourceProvinceUrl = "./output/province_source.json";
var sourceCityUrl = "./output/city_source.json";
var provinceZipedUrl = "./output/province_ziped.json";
var cityZipedUrl = "./output/city_ziped.json";
var provinceUnzipedUrl ="./output/province_unziped.json";
var cityUnzipedUrl ="./output/city_unziped.json";

var col_province = 'province_10';
var col_city = 'city_10';

var sourceUrl = (displayLevel == 0) ? sourceProvinceUrl : sourceCityUrl;
var zipedUrl = (displayLevel == 0) ? provinceZipedUrl : cityZipedUrl;
var unzipedUrl = (displayLevel == 0) ? provinceUnzipedUrl : cityUnzipedUrl;
var collection = (displayLevel == 0) ? col_province : col_city;


MongoClient.connect(url, function(err, db) {
  db.collection(collection).find({}).toArray(function(err, items) {
    var source = {}
    var ziped = {encodeScale: encodeScale}
    var unZiped = {encodeScale: encodeScale}
    _.each(items, function(item) {
      var zipedPolygon = encode(item.coordinates, encodeScale);
      var unZipedPolygon = decode(zipedPolygon.coordinates, zipedPolygon.encodeOffsets, encodeScale);

      source[item.pid] = {
        pid: item.pid,
        name: item.name,
        geometryType: item.geometry_type,
        coordinates: item.coordinates
      }
      ziped[item.pid] = {
        pid: item.pid,
        name: item.name,
        geometryType: item.geometry_type,
        coordinates: zipedPolygon.coordinates,
        encodeOffsets: zipedPolygon.encodeOffsets
      }
      unZiped[item.pid] = {
        pid: item.pid,
        name: item.name,
        geometryType: item.geometry_type,
        coordinates: unZipedPolygon.coordinates
      }
    });

    // console.log(result)
    fs.writeFile(sourceUrl, JSON.stringify(source));
    fs.writeFile(zipedUrl, JSON.stringify(ziped));
    fs.writeFile(unzipedUrl, JSON.stringify(unZiped));
    db.close();
  })
});

function encode(coordinates, encodeScale) {
  var result = {
    coordinates: [],
    encodeOffsets: []
  }

  encodeCoord(coordinates, result.coordinates, result.encodeOffsets, encodeScale);
  return result;
}

function decode(coordinates, encodeOffsets, encodeScale) {
  var result = {
    coordinates: []
  };

  decodeCoord(result.coordinates, coordinates, encodeOffsets, encodeScale);

  return result;
}

function encodeCoord(coordinates, coordResult, encodeOffsetResult, encodeScale) {
  // var coordResult = result.coordinates;
  // var encodeOffsetResult = result.encodeOffsets;
  _.each(coordinates, function(coordinate) {
    if (_.isArray(coordinate) && coordinate.length && _.isArray(coordinate[0]) && coordinate[0].length) {

      if (_.isNumber(coordinate[0][0])) {
        var enCodeResult = encodePolygon(coordinate, encodeScale);
        coordResult.push(enCodeResult.coordinates);
        encodeOffsetResult.push(enCodeResult.encodeOffsets)
      } else if (_.isArray(coordinate[0][0])) {
        var subCoordArr = [];
        var subEncodeOffsetArr = [];
        coordResult.push(subCoordArr);
        encodeOffsetResult.push(subEncodeOffsetArr);
        encodeCoord(coordinate, subCoordArr, subEncodeOffsetArr, encodeScale);
      }
    }
  });
}

function decodeCoord(coordResult, coordinates, encodeOffsets, encodeScale) {
  if (!coordResult && coordResult.length === 0) {
    return;
  }

  _.each(coordinates, function(currentCoordinate, i) {
      var currentEncodeOffsets = encodeOffsets[i];

      if (_.isArray(currentCoordinate)) {
        var subArr = [];
        coordResult.push(subArr);
        decodeCoord(subArr, currentCoordinate, encodeOffsets[i], encodeScale);
      } else if (_.isString(currentCoordinate)) {
        var coordinate = decodePolygon(currentCoordinate, currentEncodeOffsets, encodeScale);
        coordResult.push(coordinate);
      }
    })
    // for(var i=0, len=coordinates.length; i< len; i++){
    //   var currentCoordinate = coordinates[i];
    //   var currentEncodeOffsets = encodeOffsets[i];

  //   if(_.isArray(currentCoordinate)){
  //     var subArr = [];
  //     coordResult.push(subArr);
  //     decodeCoord(subArr, currentCoordinate, encodeOffsets[i], encodeScale);
  //   }else if(_.isString(currentCoordinate)){
  //     var coordinate = decodePolygon(currentCoordinate, currentEncodeOffsets, encodeScale);
  //     coordResult.push(coordinate);
  //   }
  // }
}

function encode32(num) {
  return (num << 1) ^ (num >> 31)
}

function decode32(num) {
  return (num >>> 1) ^ -(num & 1)
}

function encodePolygon(coordinates, encodeScale) {
  if (!encodeScale) {
    encodeScale = 50000;
  }
  var prevX = 0;
  var prevY = 0;
  var arr = [];

  function roundCoord(coord) {
    return Math.round(coord * encodeScale);
  }

  for (var i = coordinates.length - 1; i >= 0; i--) {
    var x;
    var y;

    if (i > 0) {
      x = roundCoord(coordinates[i][0]) - roundCoord(coordinates[i - 1][0]);
      y = roundCoord(coordinates[i][1]) - roundCoord(coordinates[i - 1][1]);
    } else {
      x = roundCoord(coordinates[i][0]) - roundCoord(coordinates[coordinates.length - 1][0]);
      y = roundCoord(coordinates[i][1]) - roundCoord(coordinates[coordinates.length - 1][1]);
      prevX = roundCoord(coordinates[coordinates.length - 1][0]);
      prevY = roundCoord(coordinates[coordinates.length - 1][1]);
    }

    x = encode32(x);
    y = encode32(y);
    // console.log(x, y)
    var str = [String.fromCharCode(x), String.fromCharCode(y)].join('');
    arr.unshift(str);
  }

  return {
    coordinates: arr.join(''),
    encodeOffsets: [prevX, prevY]
  }
}

function decodePolygon(coordinate, encodeOffsets, encodeScale) {
  if (!encodeScale) {
    encodeScale = 100000;
  }
  var result = [];
  var prevX = encodeOffsets[0];
  var prevY = encodeOffsets[1];

  for (var i = 0; i < coordinate.length; i += 2) {
    var x = coordinate.charCodeAt(i);
    var y = coordinate.charCodeAt(i + 1);

    // ZigZag decoding
    x = (x >>> 1) ^ (-(x & 1));
    y = (y >>> 1) ^ (-(y & 1));
    // Delta deocding
    x += prevX;
    y += prevY;

    prevX = x;
    prevY = y;
    // Dequantize
    result.push([x / encodeScale, y / encodeScale]);
  }

  return result;
}

var a = [
  [109.58819, 26.74999998],
  [109.57781, 26.73933999],
  [109.5848, 26.72969997],
  [109.56027, 26.71894998],
  [109.54092, 26.73242004],
  [109.54543, 26.75379004],
  [109.55675, 26.75679004],
  [109.56664, 26.75182997],
  [109.58149, 26.76272002],
  [109.59537, 26.75612997],
  [109.58819, 26.74999998]
];
// var a = [
//   [1, 6],
//   [2, 7],
//   [3, 8],
//   [4, 9],
//   [5, 10]
// ];

// var b = encodePolygon(a);
// var c = decodePolygon(b.coordinate, b.encodeOffsets);
// console.log(a)
// console.log(c)
// console.log(b)