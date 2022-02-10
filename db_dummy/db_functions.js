// import {con2, con3} from "./db_connections";
// import {con1} from "./dblocal_conn.js";
const connections = require('./db_connections');
// const con1 = connections.con1;
const con2 = connections.con2;
const con3 = connections.con3

const con1 = require('./dblocal_conn');
const mysql = require('mysql')
const lostConn = 'PROTOCOL_CONNECTION_LOST';
function closeConnection(con){
    con.end(function(err){
        if (err) throw err;
        console.log("Closed connection");
    });
}

function searchRecord(field, value){
    let query = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE ? = ?;";
    let values = [field, value];
    query = mysql.format(query, values);
    console.log(query);
    con1.connect(function(err){
        
        if(err){
            //check if err is because of server down
            if(err.code != lostConn) throw err;
        }
    
        console.log("Node 1: Connected");
        
        con1.query(query, function(err, results){
            console.log(results);
            if(err) throw err;
            if(results) return results;
        });
        
        // con2.connect(function(err){
        //     console.log("Node 2: Connected");
            
        //     if(err) throw err;
        //     con2.query(query, function(err, results){
        //         if(err) throw err;
        //         if(results) return results;
        //     } );
        // });
        
        // con3.connect(function(err){            
        //     console.log("Node 3: Connected");

        //     if(err) throw err;
        //     con3.query(query, function(err, results){
        //         if(err) throw err;
        //         if(results) return results;
        //     } );
        // });
    })
};

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

function updateOneRecordInAllNodes(id, name, year, genre, director){
    // TODO: deal with situations when year is updated
    values = [name, year, genre, director, id];

    // update entry in node 1
    con1.connect(function(err){
        if(err) throw err;
        console.log("Node 1: Connected");

        con1.query("UPDATE movies_all SET name=?, year=?, genre=?, director=? WHERE id=?;", values, function(err, results){
            if(err) throw err;
            console.log("Node 1: updated " + results.affectedRows + " records");

            // update entry in node 2
            if(year < 1980){
                con2.connect(function(err){
                    if(err) throw err;
                    console.log("Node 2: Connected");

                    con2.query("UPDATE movies_pre1980 SET name=?, year=?, genre=?, director=? WHERE id=?;", values, function(err){
                        if(err) throw err;
                        console.log("Node 2: updated " + results.affectedRows + " records");
                        closeConnection(con2);
                    });
                });
            };


            // update entry in node 3
            if(year >= 1980){
                con3.connect(function(err){
                    if(err) throw err;
                    console.log("Node 3: Connected");

                    con3.query("UPDATE movies_post1980 SET name=?, year=?, genre=?, director=? WHERE id=?;", values, function(err){
                        if(err) throw err;
                        console.log("Node 3: updated " + results.affectedRows + " records");
                        closeConnection(con3);
                    });
                });
            };

            closeConnection(con1);
        });
    });
}
module.exports = {closeConnection, searchRecord, insertOneRecordIntoAllNodes};
