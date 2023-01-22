var sqldb = require('./dbconnect');
const csvtojson = require("csvtojson");
var express = require("express");
var app = express();
var _ = require("lodash");

var fs = require("fs");


var MySQLConPool = sqldb.MySQLConPool;






var Qry = `SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS   zz__yashi_cgn;
DROP TABLE IF EXISTS  zz__yashi_cgn_data;
DROP TABLE IF EXISTS  zz__yashi_order;
DROP TABLE IF EXISTS  zz__yashi_order_data;
DROP TABLE IF EXISTS  zz__yashi_creative;
DROP TABLE IF EXISTS  zz__yashi_creative_data;
SET FOREIGN_KEY_CHECKS=1;




CREATE TABLE zz__yashi_cgn (
campaign_id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
yashi_campaign_id INT(11) UNSIGNED DEFAULT NULL,
name VARCHAR(255) DEFAULT NULL,
yashi_advertiser_id INT(11) UNSIGNED DEFAULT NULL,
advertiser_name VARCHAR(100) DEFAULT NULL,
PRIMARY KEY (campaign_id)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

CREATE TABLE zz__yashi_cgn_data (
    id INT(11) NOT NULL AUTO_INCREMENT,
    campaign_id INT(11) UNSIGNED DEFAULT NULL,
    log_date DATE DEFAULT NULL,
    impression_count INT(11) DEFAULT NULL,
    click_count INT(11) DEFAULT NULL,
    25viewed_count INT(11) DEFAULT NULL,
    50viewed_count INT(11) DEFAULT NULL,
    75viewed_count INT(11) DEFAULT NULL,
    100viewed_count INT(11) DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY campaign_id_UNIQUE (campaign_id,log_date),
    KEY fk_zz__yashi_cgn_data_campaign_id_idx (campaign_id),
    CONSTRAINT fk_zz__yashi_cgn_campaign_id FOREIGN KEY (campaign_id) REFERENCES zz__yashi_cgn (campaign_id) ON DELETE CASCADE ON UPDATE NO ACTION
  ) ENGINE=INNODB DEFAULT CHARSET=utf8;

  
CREATE TABLE zz__yashi_order (
    order_id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
    campaign_id INT(11) UNSIGNED DEFAULT NULL,
    yashi_order_id INT(20) DEFAULT NULL,
    name VARCHAR(200) DEFAULT NULL,
    PRIMARY KEY (order_id),
    KEY fk_zz__yashi_order_campaign_id_idx (campaign_id),
    CONSTRAINT fk_zz__yashi_order_campaign_id FOREIGN KEY (campaign_id) REFERENCES zz__yashi_cgn (campaign_id) ON DELETE CASCADE ON UPDATE NO ACTION
  ) ENGINE=INNODB DEFAULT CHARSET=utf8;
  
  
  
  CREATE TABLE zz__yashi_order_data (
    id INT(11) NOT NULL AUTO_INCREMENT,
    order_id INT(11) UNSIGNED DEFAULT NULL,
    log_date DATE DEFAULT NULL,
    impression_count INT(11) DEFAULT NULL,
    click_count INT(11) DEFAULT NULL,
    25viewed_count INT(11) DEFAULT NULL,
    50viewed_count INT(11) DEFAULT NULL,
    75viewed_count INT(11) DEFAULT NULL,
    100viewed_count INT(11) DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY order_id (order_id,log_date),
    KEY fk_zz__yashi_order_data_order_id_idx (order_id),
    CONSTRAINT fk_zz__yashi_order_data_order_id FOREIGN KEY (order_id) REFERENCES zz__yashi_order (order_id) ON DELETE CASCADE ON UPDATE NO ACTION
  ) ENGINE=INNODB DEFAULT CHARSET=utf8;
  
  
  
  
  CREATE TABLE zz__yashi_creative (
    creative_id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id INT(11) UNSIGNED DEFAULT NULL,
    yashi_creative_id INT(11) DEFAULT NULL,
    name VARCHAR(255) DEFAULT NULL,
    preview_url VARCHAR(255) DEFAULT NULL,
    PRIMARY KEY (creative_id),
    KEY fk_zz__yashi_creative_order_id_idx (order_id),
    CONSTRAINT fk_zz__yashi_creative_order_id FOREIGN KEY (order_id) REFERENCES zz__yashi_order (order_id) ON DELETE CASCADE ON UPDATE NO ACTION
  ) ENGINE=INNODB DEFAULT CHARSET=utf8;
  
  
  CREATE TABLE zz__yashi_creative_data (
    id INT(11) NOT NULL AUTO_INCREMENT,
    creative_id INT(11) UNSIGNED DEFAULT NULL,
    log_date DATE DEFAULT NULL,
    impression_count INT(11) DEFAULT NULL,
    click_count INT(11) DEFAULT NULL,
    25viewed_count INT(11) DEFAULT NULL,
    50viewed_count INT(11) DEFAULT NULL,
    75viewed_count INT(11) DEFAULT NULL,
    100viewed_count INT(11) DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY creative_id_UNIQUE (creative_id,log_date),
    KEY fk_zz__yashi_creative_data_creative_id_idx (creative_id),
    CONSTRAINT fk_zz__yashi_creative_data_creative_id FOREIGN KEY (creative_id) REFERENCES zz__yashi_creative (creative_id) ON DELETE CASCADE ON UPDATE NO ACTION
  ) ENGINE=INNODB DEFAULT CHARSET=utf8;

`
async function startReadingData() {
  const files = fs.readdirSync('./data');
  console.log(files);

  const data = files.map((file) => {
    return csvtojson({
    }).fromFile( "./data/" + file);
  });

  const finalData = await Promise.all(data);
  const impfinaldata = finalData.flat(1);
  csvtojsonFiledata(impfinaldata);
}

startReadingData();





 function csvtojsonFiledata (source) { 

      app.get("/", function(req, res) {
      res.send(source);
  });
  // console.log();

  var alldata = source.map( obj => {
    return _.transform(obj,  (result, val, key) => {
      result[key.toLowerCase().replace(/[^a-zA-Z0-9]+(.)/g, (m, chr) => chr.toUpperCase())] = val;
     });
   })





   queryPromise = () =>{
    return new Promise((resolve, reject)=>{
  
  
        MySQLConPool.query(Qry,  (error, results)=>{
            if(error){
                return reject(error);
            }
            return resolve(results);
        });
    });
  };
  







queryPromise1 = () =>{
  return new Promise((resolve, reject)=>{

    var Campaign = alldata.filter((arr, index, self) =>
    index === self.findIndex((t) => (t["campaignId"] === arr["campaignId"])))
  
    var values =  [Campaign.map(item => [item["campaignId"], item["campaignName"], item["advertiserId"], item["advertiserName"]])]
      // console.log(values);
  
      var query1 = "INSERT INTO zz__yashi_cgn (yashi_campaign_id, name, yashi_advertiser_id, advertiser_name) VALUES ?";
 
      MySQLConPool.query(query1,values,  (error, results)=>{
          if(error){
              return reject(error);
          }
          return resolve(results);
      });
  });
};

queryPromise2 = () =>{
  return new Promise((resolve, reject)=>{

    var query2 = "";
  
    const campArray = alldata.map((_arrayElement) => Object.assign({}, _arrayElement));  

  let result = campArray.reduce((acc, curr) => {
    let obj = acc.find(item => item.campaignId === curr.campaignId);
    if(obj) {
      obj.impressions = Number(obj.impressions) + Number(curr.impressions);
      obj.clicks = Number(obj.clicks) + Number(curr.clicks);
      obj["25Viewed"] = Number(obj["25Viewed"]) + Number(curr["25Viewed"]);
      obj["50Viewed"] = Number(obj["50Viewed"]) + Number(curr["50Viewed"]);
      obj["75Viewed"] = Number(obj["75Viewed"])  +Number(curr["75Viewed"]);
      obj["100Viewed"]= Number(obj["100Viewed"])+ Number(curr["100Viewed"]);
    } else {
      acc.push(curr);
    }
    return acc;
  }, []);
  
    result.map(respo => {
      var mul_qry = `INSERT INTO zz__yashi_cgn_data (campaign_id , log_date , impression_count, click_count, 25viewed_count,50viewed_count,75viewed_count,100viewed_count ) 
      SELECT  campaign_id, "${respo["date"]}" , "${respo["impressions"]}","${respo["clicks"]}" ,"${respo["25Viewed"]}" ,
      "${respo["50Viewed"]}" ,"${respo["75Viewed"]}" ,"${respo["100Viewed"]}"   FROM zz__yashi_cgn where yashi_campaign_id = "${respo["campaignId"]}";  `
      query2 = query2+ mul_qry;
    })
  

      MySQLConPool.query(query2,  (error, results)=>{
          if(error){
              return reject(error);
          }
          return resolve(results);
      });
  });
};

queryPromise3 = () =>{
  return new Promise((resolve, reject)=>{

    var Orders = alldata.filter((arr, index, self) =>
index === self.findIndex((t) => (t["orderId"] === arr["orderId"])))

var query3   = ""
Orders.map(respo => {
  var order_mul_qry = `INSERT INTO zz__yashi_order (campaign_id , yashi_order_id , name ) 
  SELECT  campaign_id, "${respo["orderId"]}" , "${respo["orderName"]}"  FROM zz__yashi_cgn where yashi_campaign_id = "${respo["campaignId"]}";  `
  query3  = query3 + order_mul_qry;
})


      MySQLConPool.query(query3,  (error, results)=>{
          if(error){
              return reject(error);
          }
          return resolve(results);
      });
  });
};

queryPromise4 = () =>{
  return new Promise((resolve, reject)=>{

    const orderArray = alldata.map((_arrayElement) => Object.assign({}, _arrayElement));  
    
let orderData = orderArray.reduce((acc, curr) => {
  let obj = acc.find(item => item.orderId === curr.orderId);
  if(obj) {
    obj.impressions = Number(obj.impressions) + Number(curr.impressions);
    obj.clicks = Number(obj.clicks) + Number(curr.clicks);
    obj["25Viewed"] = Number(obj["25Viewed"]) + Number(curr["25Viewed"]);
    obj["50Viewed"] = Number(obj["50Viewed"]) + Number(curr["50Viewed"]);
    obj["75Viewed"] = Number(obj["75Viewed"])  +Number(curr["75Viewed"]);
    obj["100Viewed"]= Number(obj["100Viewed"])+ Number(curr["100Viewed"]);
  } else {
    acc.push(curr);
  }
  return acc;
}, []);


var order_data_qry = ""
orderData.map(respo => {
    var order_data_mul_qry = `INSERT INTO zz__yashi_order_data (order_id , log_date , impression_count, click_count, 25viewed_count,50viewed_count,75viewed_count,100viewed_count ) 
    SELECT  order_id, "${respo["date"]}" , "${respo["impressions"]}","${respo["clicks"]}" ,"${respo["25Viewed"]}" ,
    "${respo["50Viewed"]}" ,"${respo["75Viewed"]}" ,"${respo["100Viewed"]}"   FROM zz__yashi_order where yashi_order_id = "${respo["orderId"]}";  `
    order_data_qry  = order_data_qry + order_data_mul_qry;
  })


      MySQLConPool.query(order_data_qry,  (error, results)=>{
          if(error){
              return reject(error);
          }
          return resolve(results);
      });
  });
};

queryPromise5 = () =>{
  return new Promise((resolve, reject)=>{

    var CreativeArrayData = alldata.filter((arr, index, self) =>
    index === self.findIndex((t) => (t["creativeId"] === arr["creativeId"])))

var creative_qry  = ""
CreativeArrayData.map(respo => {
  var creative_mul_qry = `INSERT INTO zz__yashi_creative (order_id, yashi_creative_id , name, preview_url ) 
  SELECT  order_id, "${respo["creativeId"]}" , "${respo["creativeName"]}" , "${respo["creativePreviewUrl"]}" FROM zz__yashi_order where yashi_order_id = "${respo["orderId"]}";  `
  creative_qry = creative_qry+ creative_mul_qry;
})


      MySQLConPool.query(creative_qry,  (error, results)=>{
          if(error){
              return reject(error);
          }
          return resolve(results);
      });
  });
};

queryPromise6 = () =>{
  return new Promise((resolve, reject)=>{



    var creative_data_data_qry = ""
    alldata.map(respo => {
      var creative_data_mul_qry = `INSERT INTO zz__yashi_creative_data (creative_id , log_date , impression_count, click_count, 25viewed_count,50viewed_count,75viewed_count,100viewed_count ) 
      SELECT  creative_id, "${respo["date"]}" , "${respo["impressions"]}","${respo["clicks"]}" ,"${respo["25Viewed"]}" ,
      "${respo["50Viewed"]}" ,"${respo["75Viewed"]}" ,"${respo["100Viewed"]}"   FROM zz__yashi_creative where yashi_creative_id = "${respo["creativeId"]}";  `
      creative_data_data_qry  = creative_data_data_qry + creative_data_mul_qry;
    })

    // console.log(alldata.length, creative_data_data_qry);


      MySQLConPool.query(creative_data_data_qry,  (error, results)=>{
          if(error){
              return reject(error);
          }
          return resolve(results);
      });
  });
};



async function sequentialQueries () {

try{
const result = await queryPromise();
const result1 = await queryPromise1();
const result2 = await queryPromise2();
const result3 = await queryPromise3();
const result4 = await queryPromise4();
const result5 = await queryPromise5();
const result6 = await queryPromise6();

console.log('Data Insertion Completed');
// here you can do something with the three results

} catch(error){
console.log(error)
}
}

sequentialQueries();

//     app.get("/", function(req, res) {
//       res.send(values);
//   });
// console.log(clean, clean.length, values.length);

 }

// exports.MySQLConPool = MySQLConPool;

// app.use("/nodeapp", require("./routes/routes"));



var server = app.listen(3875, function() {
    var host = server.address().address;
    var port = server.address().port;
    console.log(" API Server is listening at http://%s:%s", host, port);
});