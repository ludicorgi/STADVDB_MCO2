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

function closeConnection(con){
    con.end(function(err){
        if (err) throw err;
        console.log("Closed connection");
    });
}

function insertOneRecordIntoAllNodes(name, year, genre, director){
    values = [name, year, genre, director];

    // insert into node 1
    con1.connect(function(err){
        if(err) throw err;
        console.log("Node 1: Connected");

        con1.query("INSERT INTO movies_all (name, year, genre, director) VALUES (?,?,?,?);", values, function(err, results){
            if(err) throw err;
            console.log("Node 1: 1 record inserted");

            // append auto-generated id to values array
            values.unshift(results.insertId);
            
            // insert into node 2
            if(year < 1980){
                con2.connect(function(err){
                    if(err) throw err;
                    console.log("Node 2: Connected");

                    con2.query("INSERT INTO movies_pre1980 (id, name, year, genre, director) VALUES (?,?,?,?,?);", values, function(err){
                        if(err) throw err;
                        console.log("Node 2: 1 record inserted");
                        closeConnection(con2);
                    });
                });
            };


            // insert into node 3
            if(year >= 1980){
                con3.connect(function(err){
                    if(err) throw err;
                    console.log("Node 3: Connected");

                    con3.query("INSERT INTO movies_post1980 (id, name, year, genre, director) VALUES (?,?,?,?,?);", values, function(err){
                        if(err) throw err;
                        console.log("Node 3: 1 record inserted");
                        closeConnection(con3);
                    });
                });
            };

            closeConnection(con1);
        });
    });
}