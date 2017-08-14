var fs = require('fs');
var readline = require('linebyline');
var path = require('path');
var _ = require('underscore');

// var fileurl = "./files/select_t_from_otms_provinces_t_201703301511.csv";
var fileurl = "./files/select_t_from_otms_city_t_201703301512.csv";


var lines = readline(fileurl, {maxLineLength: 10000000, retainBuffer: false});
var outputPath = './output';

// fs.rmdirSync(tempPath);
// fs.mkdirSync(tempPath);
var response = {
  "type": "FeatureCollection",
  "features": []
}

lines.on('line', function(line, lineCount, byteCount){
  if(lineCount == 1) return;

  var feature = {
    "type": "Feature",
    "properties": {

    },
    geometry: {
      "type": "",
      "coordinates":null
    }
  };

  var gemStart = 0;
  var gemEnd = 0;
  var flag = 0
  for(var i=0; i< line.length; i++){
    if(line.charCodeAt(i) == 34){
      flag++;
      if(flag == 1){
        gemStart = i+1
      }else if(flag == 2){
        gemEnd = i
        break;
      }
    }
  }

  var gem = line.slice(gemStart,gemEnd);
  var other = line.slice(gemEnd+2).split(",");
  var returnObj = {}

  if(gem.startsWith("POLYGON")){
    feature.geometry.type = "Polygon"
    feature.geometry.coordinates = fetchGem(gem)

  }else if(gem.startsWith("MULTIPOLYGON")){
    feature.geometry.type = "MultiPolygon"
    feature.geometry.coordinates = fetchGem(gem)
  }

  feature.properties = {
    name: other[0],
    pid: parseInt(other[1].match(/\d+/)),
    transliterate_name: other[2],
    alternate_name: other[3],
    transliterate_alternate_name: other[4],
    language_code: other[5],
    alternate_language_code: other[6],
    government_code: parseInt(other[7].match(/\d+/)),
    province_code: parseInt(other[8].match(/\d+/))
  }

  function fetchGem(str){
    var start = 0;
    var end = 0;
    var flag = false;
    var deep = 0;
    var obj = {}
    var stru = []
    for(var i = 0,len = str.length; i<len; i++ ){
      if(str.charCodeAt(i) == 40){
        deep++;
        if(!obj[deep]){
         obj[deep] = []
       }
        start = i;
        stru.push("{")
      }
      if(str.charCodeAt(i) == 41){
        if(stru[stru.length-1] == "{"){
          end = i;
          var codinates = str.slice(start+1,end);
          var arr =codinates.split(",");
          arr = _.map(arr, function(item){
            var coArr = _.map(item.split(" "), function(coItem){
              return parseFloat(coItem);
            });

            return coArr;
          })
          obj[deep] = arr;
        }
        if(deep > 1){
          obj[deep - 1].push(obj[deep]);
          obj[deep] = null;
        }
        deep--;
        stru.push("}")
      }
    }
    // console.log(obj)
    return obj[1];
  }

  // console.log(JSON.stringify(feature))
  response.features.push(feature)

});

var outputUrl = "province.json";
var outputUrl = "city.json";

lines.on('end', function(){
  fs.writeFile(path.join(outputPath, outputUrl), JSON.stringify(response));
});
