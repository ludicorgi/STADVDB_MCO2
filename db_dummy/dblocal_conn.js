var mysql = require('mysql');

var con1 = mysql.createConnection({
    host: "localhost",
    port: 3310,
    user: "root",
    password: "12345",
    database: "node1"
});

module.exports = con1;