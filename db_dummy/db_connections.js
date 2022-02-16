var mysql = require('mysql');

var con1 = mysql.createConnection({
    host: "35.198.223.213",
    port: 3306,
    user: "root",
    password: "ADVANDBMPG9",
    database: "node1"
});

var con2 = mysql.createConnection({
    host: "34.96.224.7",
    port: 3306,
    user: "root",
    password: "ADVANDBMPG9",
    database: "node2"
});

var con3 = mysql.createConnection({
    host: "34.101.134.145",
    port: 3306,
    user: "root",
    password: "ADVANDBMPG9",
    database: "node3"
});

var con1Clone = mysql.createConnection({
    host: "35.198.223.213",
    port: 3306,
    user: "root",
    password: "ADVANDBMPG9",
    database: "node1"
});

var con2Clone = mysql.createConnection({
    host: "34.96.224.7",
    port: 3306,
    user: "admin",
    password: "ADVANDBMPG9",
    database: "node2"
});


var con3Clone = mysql.createConnection({
    host: "34.101.134.145",
    port: 3306,
    user: "root",
    password: "ADVANDBMPG9",
    database: "node3"
});

var con3Clone2 = mysql.createConnection({
    host: "34.101.134.145",
    port: 3306,
    user: "root",
    password: "ADVANDBMPG9",
    database: "node3"
});

function createConnectionNode3() {
    return mysql.createConnection({
        host: "34.101.134.145",
        port: 3306,
        user: "root",
        password: "ADVANDBMPG9",
        database: "node3"
    });
}

function createConnectionNode2() {
    return mysql.createConnection({
        host: "34.96.224.7",
        port: 3306,
        user: "admin",
        password: "ADVANDBMPG9",
        database: "node2"
    });
}

function createConnectionNode1() {
    return mysql.createConnection({
        host: "35.198.223.213",
        port: 3306,
        user: "root",
        password: "ADVANDBMPG9",
        database: "node1"
    });
}


module.exports = { con1, con2, con3, con1Clone, con2Clone, con3Clone, con3Clone2, createConnectionNode1, createConnectionNode2, createConnectionNode3 };