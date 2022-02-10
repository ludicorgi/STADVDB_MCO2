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

async function searchRecord(field, value){
    let query = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE ?? = ?;";
    let values = [(field), (value)];

    // search node 1
    con1.query(query, values, function(err, results){
        console.log("Node 1: Connected");
        if(err){
            if(err.code != lostConn) throw err;
        } else if(results.length > 0) {
            console.log("done1");
            closeConnection(con1)
            return results;
        }

        // search node 2
        query = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_pre1980 WHERE ?? = ?;";
        con2.query(query, values, function(err, results){
            console.log("Node 2: Connected");
            console.log(results);
            if(err){
                if(err.code != lostConn) throw err;
            }else if(results.length > 0) {
                console.log("done2");
                closeConnection(con2);
                return results;
            }

            //search node 3
            query = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_post1980 WHERE ?? = ?;";
            con3.query(query, values, function(err, results){
                console.log("Node 3: Connected");
                console.log(results);
                if(err){
                    if(err.code != lostConn) throw err;
                }else if(results.length > 0) {
                    console.log("done3");
                    closeConnection(con3)
                    return results;
                }
            } );
        } );
    });
};

function insertOneRecordIntoAllNodes(name, year, genre, director){
    values = [name, year, genre, director];

    // insert into node 1
    con1.query("INSERT INTO movies_all (name, year, genre, director) VALUES (?,?,?,?);", values, function(err, results){
        if(err) throw err;
        console.log("Node 1: 1 record inserted");

        // append auto-generated id to values array
        values.unshift(results.insertId);
        
        // insert into node 2
        if(year < 1980){
            con2.query("INSERT INTO movies_pre1980 (id, name, year, genre, director) VALUES (?,?,?,?,?);", values, function(err){
                if(err) throw err;
                console.log("Node 2: 1 record inserted");
                closeConnection(con2);
            });
        };


        // insert into node 3
        if(year >= 1980){
            con3.query("INSERT INTO movies_post1980 (id, name, year, genre, director) VALUES (?,?,?,?,?);", values, function(err){
                if(err) throw err;
                console.log("Node 3: 1 record inserted");
                closeConnection(con3);
            });
        };

        closeConnection(con1);
    });
}

function updateOneRecordInAllNodes(id, name, year, genre, director, old_year){
    var values = [name, year, genre, director, id];
    var values_for_insert = [id, name, year, genre, director];
    var oldYearIsPost1980 = old_year >= 1980;
    var newYearIsPost1980 = year >= 1980;

    if(oldYearIsPost1980 && !newYearIsPost1980){
        // update record in Node 1
        con1.query("UPDATE movies_all SET name=?, year=?, genre=?, director=? WHERE id=?;", values, function(err, results){
            if(err) throw err;
            console.log("Node 1: updated " + results.affectedRows + " records");

            // insert record into Node 2
            con2.query("INSERT INTO movies_pre1980 (id, name, year, genre, director) VALUES (?,?,?,?,?);", values_for_insert, function(err){
                if(err) throw err;
                console.log("Node 2: 1 record inserted");

                // delete record from Node 3
                con3.query("DELETE FROM movies_post1980 WHERE id=?", id, function(err, results){
                    if(err) throw err;
                    console.log("Node 3: deleted " + results.affectedRows + " records");

                    closeConnection(con3);
                });
                
                closeConnection(con2);
            });

            closeConnection(con1);
        });
    }
    else if(!oldYearIsPost1980 && newYearIsPost1980){
        // update record in Node 1
        con1.query("UPDATE movies_all SET name=?, year=?, genre=?, director=? WHERE id=?;", values, function(err, results){
            if(err) throw err;
            console.log("Node 1: updated " + results.affectedRows + " records");

            // insert record into Node 3
            con3.query("INSERT INTO movies_post1980 (id, name, year, genre, director) VALUES (?,?,?,?,?);", values_for_insert, function(err){
                if(err) throw err;
                console.log("Node 3: 1 record inserted");

                // delete record from Node 2
                con2.query("DELETE FROM movies_pre1980 WHERE id=?", id, function(err, results){
                    if(err) throw err;
                    console.log("Node 2: deleted " + results.affectedRows + " records");

                    closeConnection(con2);
                });
                
                closeConnection(con3);
            });

            closeConnection(con1);
        });
    }
    else{
        // update entry in node 1
        con1.query("UPDATE movies_all SET name=?, year=?, genre=?, director=? WHERE id=?;", values, function(err, results){
            if(err) throw err;
            console.log("Node 1: updated " + results.affectedRows + " records");

            // update entry in node 2
            if(year < 1980){
                con2.query("UPDATE movies_pre1980 SET name=?, year=?, genre=?, director=? WHERE id=?;", values, function(err){
                    if(err) throw err;
                    console.log("Node 2: updated " + results.affectedRows + " records");
                    closeConnection(con2);
                });
            };

            // update entry in node 3
            if(year >= 1980){
                if(err) throw err;
                    console.log("Node 3: Connected");

                    con3.query("UPDATE movies_post1980 SET name=?, year=?, genre=?, director=? WHERE id=?;", values, function(err){
                        if(err) throw err;
                        console.log("Node 3: updated " + results.affectedRows + " records");
                        closeConnection(con3);
                    });
            };

            closeConnection(con1);
        });
    }
}
module.exports = {closeConnection, searchRecord, insertOneRecordIntoAllNodes};
