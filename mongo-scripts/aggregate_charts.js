var fs = require('fs'),
    chart1 = JSON.parse(fs.readFileSync('./input/tmp/chart1.js', 'utf8')),
    chart2 = JSON.parse(fs.readFileSync('./input/tmp/chart2.js', 'utf8')),
    chart2total = JSON.parse(fs.readFileSync('./input/tmp/chart2-total.js', 'utf8'));

function processChart(chartData, avg) {
  var summary = [],
      output = {},
      numParameters = chartData[1].Alabama.data.length,
      numValues = chartData[1].Alabama.data[0].length;

  while(numParameters--) {
    summary.push([]);
  }

  while(numValues--) {
    summary.forEach(function(v, i) {
      summary[i][numValues] = 0;
    });
  }

  chartData.forEach(function(state) {
    var s;
    for (s in state) {
      state[s].data.forEach(function(v, i) {
        v.forEach(function(w, j) {
          summary[i][j] += w;
        });
      });
    }
  });

  // Chart #2 is percentages so we need to average the state's values instead of
  // adding them all together. We did this earlier with jq.
  if (avg) {
    summary = [
      [chart2total.percentConv2013, chart2total.percentConv2014, chart2total.percentConv2015],
      [chart2total.percentFHA2013, chart2total.percentFHA2014, chart2total.percentFHA2015],
      [chart2total.percentVA2013, chart2total.percentVA2014, chart2total.percentVA2015],
      [chart2total.percentRHS2013, chart2total.percentRHS2014, chart2total.percentRHS2015]
    ];
  }

  // Add U.S. Total
  chartData.unshift({'U.S. Total': {name:'U.S. Total', data:summary}});

  chartData.forEach(function(v, i) {
    var state;
    for (state in v) {
      output[state] = v[state];
    }
  });

  // Remove N/A records
  delete output.NA;

  return JSON.stringify(output);
}

fs.writeFile('./output/chart1.json', processChart(chart1), function(err) {
  if (err) console.error('There was a problem saving the first chart\'s data.');
});

fs.writeFile('./output/chart2.json', processChart(chart2, true), function(err) {
  if (err) console.error('There was a problem saving the second chart\'s data.');
}); 
