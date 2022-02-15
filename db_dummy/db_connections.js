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

var con1Clone = mysql.createConnection({
    host: "moves-all.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
    port: 3306,
    user: "admin",
    password: "12345678",
    database: "node1"
});

var con2Clone = mysql.createConnection({
    host: "movies-left.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
    port: 3306,
    user: "admin",
    password: "12345678",
    database: "node2"
});

var con3Clone = mysql.createConnection({
    host: "movies-right.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
    port: 3306,
    user: "admin",
    password: "12345678",
    database: "node3"
});

var con3Clone2 = mysql.createConnection({
    host: "movies-right.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
    port: 3306,
    user: "admin",
    password: "12345678",
    database: "node3"
});

function createConnectionNode3(){
    return mysql.createConnection({
        host: "movies-right.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
        port: 3306,
        user: "admin",
        password: "12345678",
        database: "node3"
    });
}

function createConnectionNode2(){
    return mysql.createConnection({
        host: "movies-left.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
        port: 3306,
        user: "admin",
        password: "12345678",
        database: "node2"
    });
}

function createConnectionNode1 () {
    return mysql.createConnection({
        host: "moves-all.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
        port: 3306,
        user: "admin",
        password: "12345678",
        database: "node1"
    });
}


module.exports = {con1, con2, con3, con1Clone, con2Clone, con3Clone, con3Clone2, createConnectionNode1, createConnectionNode2, createConnectionNode3};