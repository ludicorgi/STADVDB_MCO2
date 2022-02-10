var mysql = require('mysql');

var con1 = mysql.createConnection({
    host: "moves-all.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
    port: 3306,
    user: "admin",
    password: "12345678",
    database: "node1"
});

var con2 = mysql.createConnection({
    host: "movies-left.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
    port: 3306,
    user: "admin",
    password: "12345678",
    database: "node2"
});

var con3 = mysql.createConnection({
    host: "movies-right.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
    port: 3306,
    user: "admin",
    password: "12345678",
    database: "node3"
});

export {con1, con2, con3};