var fs = require('fs');
var readline = require('linebyline');
var path = require('path');
var _ = require('underscore');

var fileurl = "./files/otms,geo.zipcodes.csv";

var lines = readline(fileurl, {maxLineLength: 10000000, retainBuffer: false});
var outputPath = './output';
var outputUrl = 'counties';

lines.on('line', function(line, lineCount, byteCount){
  if(lineCount == 1) return;

  if(lineCount == 2){
    fs.writeFile(path.join(outputPath, outputUrl), line);
  }
  console.log(lineCount)

})