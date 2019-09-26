var express = require('express');
var MongoClient = require('mongodb').MongoClient;
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  var url = "mongodb://localhost:27017/";
  MongoClient.connect(url, function(err, db) {
    if (err) throw err;
    var dbo = db.db("nasu");
    dbo.collection("aarhus").find({}).toArray(function(err, result) {
      if (err) throw err;
      var timestamp = result[0]["timestamp"];
      for (var i in result) {
        if (result[i]["_id"] == "average_pollution")
          var average_pollution = result[i];
        else
          var important_pollution = result[i];
      }
      res.render('index', { ozone_average: average_pollution["ozone"],
        ozone_variation: average_pollution["ozone variation"],
        particulate_average: average_pollution["particulate matter"],
        particulate_variation: average_pollution["particulate matter variation"],
        carbon_average: average_pollution["carbon monoxide"],
        carbon_variation: average_pollution["carbon monoxide variation"],
        sulfur_average: average_pollution["sulfur dioxide"],
        sulfur_variation: average_pollution["sulfur dioxide variation"],
        nitrogen_average: average_pollution["nitrogen dioxide"],
        nitrogen_variation: average_pollution["nitrogen dioxide variation"],
        ozone_important: important_pollution["ozone"],
        particulate_important: important_pollution["particulate matter"],
        carbon_important: important_pollution["carbon monoxide"],
        sulfur_important: important_pollution["sulfur dioxide"],
        nitrogen_important: important_pollution["nitrogen dioxide"],
        timestamp: timestamp.substring(timestamp.indexOf(" ")+1, timestamp.length)});
      db.close();
    });
  });
});

module.exports = router;
