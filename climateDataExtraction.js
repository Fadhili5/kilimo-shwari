//Load all counties(GAUL Level 2)
var allCounties = ee.FeatureCollection("FAO/GAUL/2015.level2").filter(ee.Filter.eq('ADM0_NAME', 'Kenya'));

//Load CHIRPS rainfall dataset
var chirps = ee.ImageCollection("UCSB-CHG/CHIRPS/PENTAD");

//Load ERA5 temperature dataset
var era5 = ee.ImageCollection("ECMWF/ERA5/DAILY");

//Define time period
var startDate = ee.Date('2010-01-01');
var endDate = ee.Date('2020-12-31');

//Generate list of monthly start dates
var months = ee.List.sequence(0, endDate.difference(startDate, 'month').subtract(1))
  .map(function (monthOffset)) {
    return startDate.advance(monthOffset, 'month');
});

// Function calculating monthly climate stats for a single county
function calculateMonthlyStatsForCounty(countyFeature) {
  var countyName = countyFeature.get('ADM2_NAME');
  var countyGeometry = countyFeature.geometry();

  //Function to calculateclimate stats for each month
  var monthlyStats = months.map(function (monthStart) {
    var monthEnd = ee.Date(monthStart).advance(1, 'month');
  
  var monthlyRainfall = chirps
    .filterDate(monthStart, monthEnd)
    .filterBounds(countyGeometry)
    .select('precipitation')
    .mean();

  var monthlyTemperature = era5
    .filterDate(monthStart, monthEnd)
    .filterBounds(countyGeometry)
    .select('mean_2m_air_teperature')
    .mean();

    // Converting Kelvin to Celsius
    var monthlyTemperatureCelsius = ee.Algorithms.If(
      monthlyTemperature.bandNames().size().gt(0),
      monthlyTemperature.subtract(273.15).rename('mean_2m_air_temperature_celsius'),
      ee.Image().rename('mean_2m_air_temperature_celsius')  // Empty fallback image
    );
    monthlyTemperatureCelsius = ee.Image(monthlyTemperatureCelsius);

    //Reduce mean values over county geometry
    var rainfallValue = ee.Algorithms.If(
      monthlyRainfall.bandNames().size().gt(0),
      monthlyRainfall.reduceRegion({
        reducer: ee.Reducer.mean(),
        geometry: countyGeometry,
        scale: 5000,
        maxPixels: 1e9
      }).get('precipitation'),
      null
    );

    var temperatureValue = ee.Algorithms.If(
      monthlyTemperatureCelsius.bandNames().size().gt(0),
      monthlyTemperatureCelsius.reduceRegion({
        reducer: ee.Reducer.mean(),
        geometry: countyGeometry,
        scale: 5000,
        maxPixels: 1e9
      }).get('mean_2m_air_temperature_celsius'),
      null
    );

    //Return as feature
    return ee.Feature(null, {
      'County': countyName,
      'Month': ee.Date(monthStart).format('YYYY-MM'),
      'MeanRainfall': rainfallValue,
      'MeanTemperature': temperatureValue
    });
  });

  return ee.FeatureCollection(monthlyStats);
}

//Function to process a batch of counties
funtion processCountyBatch(counties, batchNumber) {
  var initial = ee.FeatureCollection([]);
  var batchMonthlyStats = ee.FeatureCollection(
    counties.iterate(function (county, result) {
      var resultFc = ee.FeatureCollection(result);
      var countyFc = calculateMonthlyStatsForCounty(ee.Feature(county));
      return resultFc.merge(countyFc);
    }, initial)
  );

  //Export to Google Drive as CSV
  Export.table.toDrive({
    collection: batchMonthlyStats,
    description: 'Kenya_County_Monthly_Climate_Stats_Batch' + batchNumber,
    fileFormat: 'CSV',
    selectors: ['County', 'Month', 'MeanRainfall', 'MeanTemperature']
  });
}

//Determine batch size and number of batches
var batchSize = 5;
var countyCount = allCounties.size().getInfo();
var numBatches = Math.ceil(countyCount/batchSize);

print('Total number of batches:', numBatches);

for (var batchNumber = 0; batchNumber < numBatches; batchNumber++) {
  var start = batchNumber * batchSize;
  var end = Math.min(start + batchSize, countyCount);
  var batchCounties = allCounties.toList(batchSize, start);

  processCountyBatch(ee.FeatureCollection(batchCounties), batchNumber + 1);
}
