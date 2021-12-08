const express = require("express");
var cors = require("cors");
require("dotenv").config();
const AWS = require("aws-sdk");
const app = express();

const port = 3000;
app.use(cors());
// const path = require("path");
// // const { info } = require("console");
// let publicPath = path.resolve(__dirname, "public");
// app.use(express.static(publicPath));
// // app.get('/createtable', createTable)
app.listen(port, () => console.log(`Example app listening on port ${port}!`));

var s3 = new AWS.S3({
  accessKeyId: process.env.ACCESS_KEY_ID,
  secretAccessKey: process.env.SECRET_ACCESS_KEY,
});

AWS.config.update({
  region: "eu-west-1",
  endpoint: "https://dynamodb.eu-west-1.amazonaws.com",
  accessKeyId: process.env.ACCESS_KEY_ID,
  secretAccessKey: process.env.SECRET_ACCESS_KEY,
});

// src="https://sdk.amazonaws.com/js/aws-sdk-2.7.16.min.js"

var dynamodb = new AWS.DynamoDB();

var getParams = {
  Bucket: "csu44000assign2useast20",
  Key: "moviedata.json",
};

app.post("/create", async function (req, res) {
  console.log("post function");
  let allMovies = [];
  s3.getObject(getParams, async function (err, data) {
    if (err) {
      console.log(err);
    } else {
      console.log("Test");
      allMovies = await JSON.parse(data.Body);
      var docClient = new AWS.DynamoDB.DocumentClient();

      console.log("Importing movies into DynamoDB. Please wait.");

      var params = {
        TableName: "Movies",
        KeySchema: [
          { AttributeName: "yearOfRelease", KeyType: "HASH" },
          { AttributeName: "rating", KeyType: "RANGE" },
        ],
        AttributeDefinitions: [
          { AttributeName: "yearOfRelease", AttributeType: "N" },
          { AttributeName: "rating", AttributeType: "N" },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 10,
          WriteCapacityUnits: 10,
        },
      };

      dynamodb.createTable(params, async function (err, data) {
        if (err) {
          console.log(err);
          console.error(
            "Table cannot be created",
            JSON.stringify(err, null, 2)
          );
          console.log(err);
        } else {
          // console.log("Table created", JSON.stringify(data, null, 2));

          let inProgress = true;

          var checkParams = {
            TableName: "Movies",
          };
          while (inProgress) {
            let details = await dynamodb.describeTable(checkParams).promise();
            if (details.Table.TableStatus == "ACTIVE") {
              inProgress = false;
            }
          }
          allMovies.forEach(function (movie) {
            var putParams = {
              TableName: "Movies",
              Item: {
                yearOfRelease: movie.year,
                title: movie.title,
                info: movie.info,
                rating: movie.info.rating,
              },
            };
            if (movie.year == null) {
              movie.year = 0;
            }
            if (movie.rating == null) {
              movie.rating = 0;
            }
            docClient.put(putParams, function (err, data) {
              if (err) {
                // console.error(
                //   "Unable to add movie",
                //   movie.title,
                //   movie.year,
                //   ". Error JSON:",
                //   JSON.stringify(err, null, 2)
                // );
              } else {
                // console.log("PutItem succeeded:", movie.title);
              }
            });
          });
          console.log("Movies have been imported into DynamoDB");
          res.status(200);
          res.json({});
        }
      });
    }
  });
});

app.get("/query", async function (req, res) {
  let movie = req.query["movie"];
  let year = req.query["year"];
  let rating = req.query["rating"];
  console.log(movie);
  console.log(year);
  console.log(rating);

  var queryParams = {
    TableName: "Movies",
    FilterExpression: "contains(title, :name)",

    ExpressionAttributeValues: {
      ":yr": { N: year },
      ":rating": { N: rating },
      ":name": { S: movie },
    },
    KeyConditionExpression: "yearOfRelease = :yr and rating >= :rating",
  };

  dynamodb.query(queryParams, function (err, data) {
    if (err) {
      console.error("Unable to query. Error:", JSON.stringify(err, null, 2));
    } else {
      console.log("Query succeeded.");
      // data.Items.forEach(function (item) {
      //   console.log(item.title);
      //   console.log(item.yearOfRelease);
      //   console.log(item.rating);
      // });
    }
    console.log("Table has been queried");
    res.status(200);
    res.json({
      results: data.Items,
    });
  });
});

app.delete("/delete", async function (req, res) {
  var params = {
    TableName: "Movies",
  };

  dynamodb.deleteTable(params, function (err, data) {
    if (err) {
      console.error(
        "Unable to delete table. Error JSON:",
        JSON.stringify(err, null, 2)
      );
    } else {
      console.log(
        "Deleted table. Table description JSON:"
        // JSON.stringify(data, null, 2)
      );
      res.status(200);
      res.json({});
    }
  });
});
