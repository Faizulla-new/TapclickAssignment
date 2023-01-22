var mysql = require("mysql");
var MySQLConnection = {};
var MySQLConPool	= {};

var USER = "root";
var PWD = "";
var DATABASE = "yashi_db";
var DB_HOST_NAME = "localhost";


var MAX_POOL_SIZE		= 100;
var MIN_POOL_SIZE		= 50;

var MySQLConPool = mysql.createPool({
    host                : DB_HOST_NAME,
    port      		    : 3306,
    user                : USER,
    password            : PWD,
    database            : DATABASE,
    connectTimeout		: 20000,
    connectionLimit	    : MAX_POOL_SIZE,
    debug 		        : false,
    multipleStatements  : true
});

// var rl = readline.createInterface({
//     input: fs.createReadStream('./yashi_tables.sql'),
//     terminal: false
//    });
//   rl.on('line', function(chunk){
//       MySQLConPool.query(chunk.toString('ascii'), function(err, sets, fields){
//        if(err) console.log(err);
//       });
//   });
//   rl.on('close', function(){
//     console.log("finished");
//     // MySQLConPool.end();
//   });

// var Qry = `
// SET FOREIGN_KEY_CHECKS=0;
//   DROP TABLE  zz__yashi_cgn;
//   DROP TABLE  zz__yashi_cgn_data;
//   DROP TABLE  zz__yashi_order;
//   DROP TABLE  zz__yashi_order_data;
//   DROP TABLE  zz__yashi_creative;
//   DROP TABLE  zz__yashi_creative_data;
// `




exports.MySQLConPool = MySQLConPool;