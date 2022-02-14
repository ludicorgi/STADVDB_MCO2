// import {con2, con3} from "./db_connections";
// import {con1} from "./dblocal_conn.js";
const connections = require('./db_connections');
const con1 = connections.con1;
const con2 = connections.con2;
const con3 = connections.con3

// const con1 = require('./dblocal_conn');
const mysql = require('mysql')
const lostConn = 'PROTOCOL_CONNECTION_LOST';
function closeConnection(con) {
    con.end(function (err) {
        if (err) throw err;
        console.log("Closed connection");
    });
}

async function searchRecord(field, value) {
    let query = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE ?? = ?;";
    let values = [(field), (value)];

    // search node 1
    con1.query(query, values, function (err, results) {
        console.log("Node 1: Connected");
        if (err) {
            if (err.code != lostConn) throw err;
        } else if (results.length > 0) {
            console.log("done1");
            closeConnection(con1)
            return results;
        }

        // search node 2
        query = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_pre1980 WHERE ?? = ?;";
        con2.query(query, values, function (err, results) {
            console.log("Node 2: Connected");
            console.log(results);
            if (err) {
                if (err.code != lostConn) throw err;
            } else if (results.length > 0) {
                console.log("done2");
                closeConnection(con2);
                return results;
            }

            //search node 3
            query = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_post1980 WHERE ?? = ?;";
            con3.query(query, values, function (err, results) {
                console.log("Node 3: Connected");
                console.log(results);
                if (err) {
                    if (err.code != lostConn) throw err;
                } else if (results.length > 0) {
                    console.log("done3");
                    closeConnection(con3)
                    return results;
                }
            });
        });
    });
};

function insertOneRecordIntoAllNodes(name, year, rank, genre, director) {
    values = [name, year, rank, genre, director];

    // insert into node 1
    con1.query("INSERT INTO movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", values, function (err, results) {
        if (err) throw err;
        console.log("Node 1: 1 record inserted");

        // append auto-generated id to values array
        values.unshift(results.insertId);

        // insert into node 2
        if (year < 1980) {
            con2.query("INSERT INTO movies_pre1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values, function (err) {
                if (err) throw err;
                console.log("Node 2: 1 record inserted");
                closeConnection(con2);
            });
        };


        // insert into node 3
        if (year >= 1980) {
            con3.query("INSERT INTO movies_post1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values, function (err) {
                if (err) throw err;
                console.log("Node 3: 1 record inserted");
                closeConnection(con3);
            });
        };

        closeConnection(con1);
    });
}

function updateOneRecordInAllNodes(id, name, year, rank, genre, director, old_year) {
    var values = [name, year, rank, genre, director, id];
    var values_for_insert = [id, name, year, rank, genre, director];
    var oldYearIsPost1980 = old_year >= 1980;
    var newYearIsPost1980 = year >= 1980;

    if (oldYearIsPost1980 && !newYearIsPost1980) {
        // update record in Node 1
        con1.query("UPDATE movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function (err, results) {
            if (err) throw err;
            console.log("Node 1: updated " + results.affectedRows + " records");

            // insert record into Node 2
            con2.query("INSERT INTO movies_pre1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values_for_insert, function (err) {
                if (err) throw err;
                console.log("Node 2: 1 record inserted");

                // delete record from Node 3
                con3.query("DELETE FROM movies_post1980 WHERE id=?", id, function (err, results) {
                    if (err) throw err;
                    console.log("Node 3: deleted " + results.affectedRows + " records");

                    closeConnection(con3);
                });

                closeConnection(con2);
            });

            closeConnection(con1);
        });
    }
    else if (!oldYearIsPost1980 && newYearIsPost1980) {
        // update record in Node 1
        con1.query("UPDATE movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function (err, results) {
            if (err) throw err;
            console.log("Node 1: updated " + results.affectedRows + " records");

            // insert record into Node 3
            con3.query("INSERT INTO movies_post1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values_for_insert, function (err) {
                if (err) throw err;
                console.log("Node 3: 1 record inserted");

                // delete record from Node 2
                con2.query("DELETE FROM movies_pre1980 WHERE id=?", id, function (err, results) {
                    if (err) throw err;
                    console.log("Node 2: deleted " + results.affectedRows + " records");

                    closeConnection(con2);
                });

                closeConnection(con3);
            });

            closeConnection(con1);
        });
    }
    else {
        // update entry in node 1
        con1.query("UPDATE movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function (err, results) {
            if (err) throw err;
            console.log("Node 1: updated " + results.affectedRows + " records");

            // update entry in node 2
            if (year < 1980) {
                con2.query("UPDATE movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function (err) {
                    if (err) throw err;
                    console.log("Node 2: updated " + results.affectedRows + " records");
                    closeConnection(con2);
                });
            };

            // update entry in node 3
            if (year >= 1980) {
                con3.query("UPDATE movies_post1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function (err) {
                    if (err) throw err;
                    console.log("Node 3: updated " + results.affectedRows + " records");
                    closeConnection(con3);
                });
            };

            closeConnection(con1);
        });
    }
}
function setIsolationLevel(con, isolationLevel) {
    isolationLevel = isolationLevel.toLowerCase();
    if (isolationLevel == 'read uncommitted') {
        con.query('SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;', (err) => {
            console.log("READ UNCOMMITTED");
            if (err) throw err;
        })
    } else if (isolationLevel == 'read committed') {
        con.query('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;', (err) => {
            console.log("READ COMMITTED");
            if (err) throw err;
        })
    } else if (isolationLevel == 'repeatable read') {
        con.query('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;', (err) => {
            console.log("REPEATABLE READ");
            if (err) throw err;
        })
    } else if (isolationLevel == 'serializable') {
        con.query('SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;', (err) => {
            console.log("SERIALIZABLE");
            if (err) throw err;
        })
    }
}
function setAllIsolationLevel(isolationLevel) {
    setIsolationLevel(con1, isolationLevel);
    setIsolationLevel(con2, isolationLevel);
    setIsolationLevel(con3, isolationLevel);
    setIsolationLevel(connections.con1Clone, isolationLevel);
    setIsolationLevel(connections.con2Clone, isolationLevel);
    setIsolationLevel(connections.con3Clone, isolationLevel);
}

function newSearch(field, value) {
    let queryNode1 = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE ?? = ?;";
    let queryNode2 = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_pre1980 WHERE ?? = ?;";
    let queryNode3 = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_post1980 WHERE ?? = ?;";

    let values = [(field), (value)];

    // search node 1
    con1.beginTransaction((errNode1) => {
        if (errNode1) {
            // check other nodes
            if (field == 'year') {
                if (value > 1980) {
                    //node3
                    con3.beginTransaction((errNode3) => {
                        if (errNode3) throw err;
                        else {
                            // transaction began
                            con3.query("LOCK TABLES movies_post1980 READ", (err) => {
                                if (err) throw err;
                                else {
                                    // locking was successful
                                    con3.query(queryNode3, values, (err, res) => {
                                        if (err) {
                                            con3.rollback((err) => {
                                                if (err) throw err;
                                                closeConnection(con3);
                                            });
                                        } else {
                                            // query was successful
                                            con3.query("UNLOCK TABLES", (err) => {
                                                if (err) {
                                                    con3.rollback((err) => {
                                                        if (err) throw err;
                                                        closeConnection(con3);
                                                    });
                                                } else {
                                                    // unlocking was successful
                                                    con3.commit((err) => {
                                                        if (err) con3.rollback((err) => {
                                                            if (err) throw err;
                                                            closeConnection(con3);
                                                        });
                                                        // commit was successful
                                                        closeConnection(con3);
                                                        return callback(res);
                                                    });
                                                };
                                            });
                                        };
                                    });
                                };
                            });
                        };
                    });
                } else {
                    //node2
                    con2.beginTransaction((errNode2) => {
                        if (errNode2) throw err;
                        else {
                            // transaction began
                            con2.query("LOCK TABLES movies_pre1980 READ", (err) => {
                                if (err) throw err;
                                else {
                                    // locking successful
                                    con2.query(queryNode2, values, (err, res) => {
                                        if (err) {
                                            con2.rollback((err) => {
                                                if (err) throw err;
                                                closeConnection(con2);
                                            });
                                        } else {
                                            // query successful
                                            con2.query("UNLOCK TABLES", (err) => {
                                                if (err) {
                                                    con2.rollback((err) => {
                                                        if (err) throw err;
                                                        closeConnection(con2);
                                                    });
                                                } else {
                                                    // unlocking successful
                                                    con2.commit((err) => {
                                                        if (err) con2.rollback((err) => {
                                                            if (err) throw err;
                                                        });
                                                        closeConnection(con2);
                                                        return callback(res);
                                                    });
                                                };
                                            });
                                        };
                                    });
                                };
                            });
                        };
                    });
                };
            }else{
                // field is not year
                con2.beginTransaction((errNode2) => {
                    if (errNode2) throw err;
                    else {
                        // transaction began
                        con2.query("LOCK TABLES movies_pre1980 READ", (err) => {
                            if (err) throw err;
                            else {
                                // locking successful
                                con2.query(queryNode2, values, (err, res) => {
                                    if (err) {
                                        con2.rollback((err) => {
                                            if (err) throw err;
                                            closeConnection(con2);
                                        });
                                    } else if(res.length > 0){
                                        // result is in node
                                        con2.query("UNLOCK TABLES", (err) => {
                                            if (err) {
                                                con2.rollback((err) => {
                                                    if (err) throw err;
                                                    closeConnection(con2);
                                                });
                                            } else {
                                                // unlock successful
                                                con2.commit((err) => {
                                                    if (err) con2.rollback((err) => {
                                                        if (err) throw err;
                                                    });
                                                    closeConnection(con2)
                                                    return callback(res);
                                                });
                                            };
                                        });
                                    } else {
                                        // not in this node
                                        con2.rollback((err) => {
                                            if(err) throw err;
                                            closeConnection(con2);
                                        });
                                    };
                                });
                            };
                        });
                    };
                });

                con3.beginTransaction((errNode3) => {
                    if (errNode3) throw err;
                    else {
                        // transaction began
                        con3.query("LOCK TABLES movies_post1980 READ", (err) => {
                            if (err) throw err;
                            else {
                                // locking succesful
                                con3.query(queryNode3, values, (err, res) => {
                                    if (err) {
                                        con3.rollback((err) => {
                                            if (err) throw err;
                                            closeConnection(con3);
                                        });
                                    } else if(res.length > 0){
                                        // is in this node
                                        con3.query("UNLOCK TABLES", (err) => {
                                            if (err) {
                                                con3.rollback((err) => {
                                                    if (err) throw err;
                                                    closeConnection(con3);
                                                });
                                            } else {
                                                // unlocking successful
                                                con3.commit((err) => {
                                                    if (err) con3.rollback((err) => {
                                                        if (err) throw err;
                                                    });
                                                    closeConnection(con3);
                                                    return callback(res);
                                                });
                                            };
                                        });
                                    } else {
                                        // not in this node
                                        con3.rollback((err) => {
                                            if(err) throw err;
                                            closeConnection(con3);
                                        });
                                    };
                                });
                            };
                        });
                    };
                });
            }
        } else {
            con1.query("LOCK TABLES movies_all READ", (err) => {
                if (err) {
                    con1.rollback((err) => {
                        if (err) throw err;
                        closeConnection(con1);
                    });
                } else {
                    // locking successful
                    con1.query(queryNode1, values, (err, res) => {
                        if (err) {
                            con1.rollback((err) => {
                                if (err) throw err;
                                closeConnection(con1);
                            });
                        } else {
                            // query successful
                            con1.query("UNLOCK TABLES", (err) => {
                                if (err) {
                                    con1.rollback((err) => {
                                        if (err) throw err;
                                        closeConnection(con1);
                                    });
                                } else {
                                    // unlock successful
                                    con1.commit((err) => {
                                        if (err) throw err;
                                        // commit successful
                                        closeConnection(con1);
                                        return callback(res);
                                    });
                                };
                            });
                        };
                    });
                };
            });
        };
    });
};

module.exports = { closeConnection, searchRecord, insertOneRecordIntoAllNodes, setIsolationLevel, setAllIsolationLevel, newSearch};
