// import {con2, con3} from "./db_connections";
// import {con1} from "./dblocal_conn.js";
const connections = require('./db_connections');
const con1 = connections.con1;
const con2 = connections.con2;
const con3 = connections.con3

// const con1 = require('./dblocal_conn');
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

function insertOneRecordIntoAllNodes(name, year, rank, genre, director){
    values = [name, year, rank, genre, director];

    // insert into node 1
    con1.query("INSERT INTO movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", values, function(err, results){
        if(err) throw err;
        console.log("Node 1: 1 record inserted");

        // append auto-generated id to values array
        values.unshift(results.insertId);
        
        // insert into node 2
        if(year < 1980){
            con2.query("INSERT INTO movies_pre1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values, function(err){
                if(err) throw err;
                console.log("Node 2: 1 record inserted");
                closeConnection(con2);
            });
        };

        // insert into node 3
        if(year >= 1980){
            con3.query("INSERT INTO movies_post1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values, function(err){
                if(err) throw err;
                console.log("Node 3: 1 record inserted");
                closeConnection(con3);
            });
        };

        closeConnection(con1);
    });
}

function newInsert(name, year, rank, genre, director){
    values = [name, year, rank, genre, director];

    // insert into node 1
    con1.query("INSERT INTO movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", values, function(errNode1, results){
        if(errNode1){
            // TODO: create system where id can be retrieved even when node 1 is down
            values.unshift(400000);

            // insert into node 3
            if(year >= 1980){
                con3.query("INSERT INTO movies_post1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values, function(errNode3){
                    if(errNode3){
                        throw errNode3;
                    }

                    console.log("Node 3: 1 record inserted to movies_post1980");
                    values.unshift(0, "INSERT");
                    con3.query("INSERT INTO fail_log (resolved, type, movie_id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?,?)", values, function(err){
                        if(err) throw err;
                        console.log("Node 3: 1 record inserted to fail_log");
                        closeConnection(con3);
                        throw errNode1;
                    })
                });
            };

            // insert into node 2
            if(year < 1980){
                con2.query("INSERT INTO movies_pre1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values, function(errNode2){
                    if(errNode2){
                        throw errNode2;
                    }

                    console.log("Node 2: 1 record inserted to movies_pre1980");
                    values.unshift(0, "INSERT");
                    con2.query("INSERT INTO fail_log (resolved, type, movie_id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?,?)", values, function(err){
                        if(err) throw err;
                        console.log("Node 2: 1 record inserted to fail_log");
                        closeConnection(con2);
                        throw errNode1;
                    })
                });
            };
        }
        else{
            console.log("Node 1: 1 record inserted to movies_all");
            // append auto-generated id to values array
            values.unshift(results.insertId);

            // insert into node 2
            if(year < 1980){
                con2.query("INSERT INTO movies_pre1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values, function(errNode2){
                    if(errNode2){
                        values.unshift(0, "INSERT");
                        con1.query("INSERT INTO fail_log (resolved, type, movie_id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?,?)", values, function(err){
                            if(err){
                                throw err;
                            } 
                            console.log("Node 1: 1 record inserted to fail_log");
                            throw errNode2;
                        });
                    } 
                    else{
                        console.log("Node 2: 1 record inserted to movies_pre1980");
                        closeConnection(con2);
                    }
                    closeConnection(con1);
                });
            };

            // insert into node 3
            if(year >= 1980){
                con3.query("INSERT INTO movies_post1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values, function(errNode3){
                    if(errNode3){
                        values.unshift(0, "INSERT");
                        con1.query("INSERT INTO fail_log (resolved, type, movie_id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?,?)", values, function(err){
                            if(err){
                                throw err;
                            } 
                            console.log("Node 1: 1 record inserted to fail_log");
                            throw errNode3;
                        });
                    } 
                    else{
                        console.log("Node 3: 1 record inserted to movies_post1980");
                        closeConnection(con3);
                    }
                    closeConnection(con1);
                });
            };
        }
    });
}

function reallyNewInsert(name, year, rank, genre, director){
    con1.beginTransaction(function(err1){
        con1.query("INSERT INTO recovery_log (type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", ['START', 'startTxn', 0, 0, 'startTxn', 'startTxn'], function(err1, results){
            if(err1){
                return con1.rollback(function(){
                    throw err1;
                });
            }
            
            var txnId = results.insertId;
            con1.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'INSERT', name, year, rank, genre, director], function(err1){
                if(err1){
                    return con1.rollback(function(){
                        throw err1;
                    });
                }

                con1.query("INSERT INTO final_movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function(err1){
                    if(err1){
                        return con1.rollback(function(){
                            throw err1;
                        });
                    }
                    
                    if(year < 1980){
                        con2.beginTransaction(function(err2){
                            if(err2){
                                return con2.rollback(function(){
                                    throw err2;
                                });
                            }

                            con2.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'START', 'startTxn', 0, 0, 'startTxn', 'startTxn'], function(err2){
                                if(err2){
                                    return con2.rollback(function(){
                                        throw err2;
                                    });
                                }

                                con2.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'INSERT', name, year, rank, genre, director], function(err2){
                                    if(err2){
                                        return con2.rollback(function(){
                                            throw err2;
                                        });
                                    }
    
                                    con2.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function(err2){
                                        if(err2){
                                            return con2.rollback(function(){
                                                throw err2;
                                            });
                                        }
    
                                        con1.commit(function(err1){
                                            if(err1){
                                                return con1.rollback(function(){
                                                    throw err1;
                                                });
                                            }
    
                                            con1.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn'], function(err1) {
                                                if(err1){
                                                    return con1.rollback(function(){
                                                        throw err1;
                                                    });
                                                }
                                                closeConnection(con1);
                                            });
                                        })
                                        con2.commit(function(err2){
                                            if(err2){
                                                return con2.rollback(function(){
                                                    throw err2;
                                                });
                                            }
                                            
                                            con2.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn'], function (err2) {
                                                if(err2){
                                                    return con2.rollback(function(){
                                                        throw err2;
                                                    });
                                                }
                                                closeConnection(con2);
                                            });
                                        });
                                    });
                                });
                            });
                        });
                        
                    }

                    if(year >= 1980){
                        con3.beginTransaction(function(err3){
                            if(err3){
                                return con3.rollback(function(){
                                    throw err3;
                                });
                            }

                            con3.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'START', 'startTxn', 0, 0, 'startTxn', 'startTxn'], function (err3){
                                if(err3){
                                    return con3.rollback(function(){
                                        throw err3;
                                    });
                                }
                                
                                con3.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'INSERT', name, year, rank, genre, director], function(err3){
                                    if(err3){
                                        return con3.rollback(function(){
                                            throw err3;
                                        });
                                    }
        
                                    con3.query("INSERT INTO final_movies_post1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function(err3){
                                        if(err3){
                                            return con3.rollback(function(){
                                                throw err3;
                                            });
                                        }
        
                                        con1.commit(function(err1){
                                            if(err1){
                                                return con1.rollback(function(){
                                                    throw err1;
                                                });
                                            }
        
                                            con1.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn'], function (err1) {
                                                if(err1){
                                                    return con1.rollback(function(){
                                                        throw err1;
                                                    });
                                                }
                                                closeConnection(con1);
                                            });
                                        })
                                        con3.commit(function(err3){
                                            if(err3){
                                                return con3.rollback(function(){
                                                    throw err3;
                                                });
                                            }
        
                                            con3.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn'], function(err3){
                                                if(err3){
                                                    return con3.rollback(function(){
                                                        throw err3;
                                                    });
                                                }
                                                closeConnection(con3);
                                            });
                                        })
                                    });
                                });
                            });
                        });
                    }
                });
            });
        });
    });
}

function updateOneRecordInAllNodes(id, name, year, rank, genre, director, old_year){
    var values = [name, year, rank, genre, director, id];
    var values_for_insert = [id, name, year, rank, genre, director];
    var oldYearIsPost1980 = old_year >= 1980;
    var newYearIsPost1980 = year >= 1980;

    if(oldYearIsPost1980 && !newYearIsPost1980){
        // update record in Node 1
        con1.query("UPDATE movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(err, results){
            if(err) throw err;
            console.log("Node 1: updated " + results.affectedRows + " records");

            // insert record into Node 2
            con2.query("INSERT INTO movies_pre1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values_for_insert, function(err){
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
        con1.query("UPDATE movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(err, results){
            if(err) throw err;
            console.log("Node 1: updated " + results.affectedRows + " records");

            // insert record into Node 3
            con3.query("INSERT INTO movies_post1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values_for_insert, function(err){
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
        con1.query("UPDATE movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(err, results){
            if(err) throw err;
            console.log("Node 1: updated " + results.affectedRows + " records");

            // update entry in node 2
            if(year < 1980){
                con2.query("UPDATE movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(err){
                    if(err) throw err;
                    console.log("Node 2: updated " + results.affectedRows + " records");
                    closeConnection(con2);
                });
            };

            // update entry in node 3
            if(year >= 1980){
                con3.query("UPDATE movies_post1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(err){
                    if(err) throw err;
                    console.log("Node 3: updated " + results.affectedRows + " records");
                    closeConnection(con3);
                });
            };

            closeConnection(con1);
        });
    }
}

function newUpdate(id, name, year, rank, genre, director, old_year){
    var values = [name, year, rank, genre, director, id];
    var values_for_insert = [id, name, year, rank, genre, director];
    var oldYearIsPost1980 = old_year >= 1980;
    var newYearIsPost1980 = year >= 1980;

    if(oldYearIsPost1980 && !newYearIsPost1980){
        // update record in Node 1
        con1.query("UPDATE movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(errNode1, results){
            if(errNode1){
                con2.query("INSERT INTO movies_pre1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values_for_insert, function(err){
                    if(err) throw err;
                    console.log("Node 2: 1 record inserted");

                    con2.query("INSERT INTO fail_log (resolved, type, movie_id, name, year, `rank`, genre, director) VALUES (0,'UPDATE',?,?,?,?,?,?);", values_for_insert, function(err){
                        if (err) throw err;
                        console.log("Node 2: 1 record inserted into fail_log");
                        closeConnection(con2);
                        // delete record from Node 3
                        con3.query("DELETE FROM movies_post1980 WHERE id=?", id, function(err, results){
                            if(err) throw err;
                            console.log("Node 3: deleted " + results.affectedRows + " records");
                            closeConnection(con3);
                            throw errNode1;
                        });
                    });
                });
            } 
            else{
                console.log("Node 1: updated " + results.affectedRows + " records");

                // insert record into Node 2
                con2.query("INSERT INTO movies_pre1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values_for_insert, function(err){
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
            }
        });
    }
    else if(!oldYearIsPost1980 && newYearIsPost1980){
        // update record in Node 1
        con1.query("UPDATE movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(errNode1, results){
            if(errNode1){
                // insert record into Node 3
                con3.query("INSERT INTO movies_post1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values_for_insert, function(err){
                    if(err) throw err;
                    console.log("Node 3: 1 record inserted");

                    con3.query("INSERT INTO fail_log (resolved, type, movie_id, name, year, `rank`, genre, director) VALUES (0,'UPDATE',?,?,?,?,?,?);", values_for_insert, function(err){
                        if (err) throw err;
                        console.log("Node 3: 1 record inserted into fail_log");
                        closeConnection(con3);
                        // delete record from Node 2
                        con2.query("DELETE FROM movies_pre1980 WHERE id=?", id, function(err, results){
                            if(err) throw err;
                            console.log("Node 2: deleted " + results.affectedRows + " records");
                            closeConnection(con2);
                            throw errNode1;
                        });
                    });
                });                
            } 
            else{
                console.log("Node 1: updated " + results.affectedRows + " records");

                // insert record into Node 3
                con3.query("INSERT INTO movies_post1980 (id, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", values_for_insert, function(err){
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
            }
        });
    }
    else{
        // update entry in node 1
        con1.query("UPDATE movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(errNode1, results){
            if(errNode1){
                // update entry in node 2
                if(year < 1980){
                    con2.query("UPDATE movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(err, results){
                        if(err) throw err;
                        console.log("Node 2: updated " + results.affectedRows + " records");

                        con2.query("INSERT INTO fail_log (resolved, type, movie_id, name, year, `rank`, genre, director) VALUES (0,'UPDATE',?,?,?,?,?,?);", values_for_insert, function(err){
                            if (err) throw err;
                            console.log("Node 2: 1 record inserted into fail_log");
                            closeConnection(con2);
                            throw errNode1;
                        });
                    });
                };

                // update entry in node 3
                if(year >= 1980){
                    con3.query("UPDATE movies_post1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(err, results){
                        if(err) throw err;
                        console.log("Node 3: updated " + results.affectedRows + " records");

                        con3.query("INSERT INTO fail_log (resolved, type, movie_id, name, year, `rank`, genre, director) VALUES (0,'UPDATE',?,?,?,?,?,?);", values_for_insert, function(err){
                            if (err) throw err;
                            console.log("Node 3: 1 record inserted into fail_log");
                            closeConnection(con3);
                            throw errNode1;
                        });
                    });
                };
            }
            else{
                console.log("Node 1: updated " + results.affectedRows + " records");
                // update entry in node 2
                if(year < 1980){
                    con2.query("UPDATE movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(errNode2){
                        if(errNode2){
                            con1.query("INSERT INTO fail_log (resolved, type, movie_id, name, year, `rank`, genre, director) VALUES (0,'UPDATE',?,?,?,?,?,?);", values_for_insert, function(err){
                                if(err) throw err;
                                console.log("Node 1: 1 record inserted into fail_log");
                                closeConnection(con1);
                                throw errNode2;
                            });
                        } 
                        else{
                            console.log("Node 2: updated " + results.affectedRows + " records");
                            closeConnection(con2); 
                            closeConnection(con1);
                        }
                    });
                };

                // update entry in node 3
                if(year >= 1980){
                    con3.query("UPDATE movies_post1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE id=?;", values, function(errNode3){
                        if(errNode3){
                            con1.query("INSERT INTO fail_log (resolved, type, movie_id, name, year, `rank`, genre, director) VALUES (0,'UPDATE',?,?,?,?,?,?);", values_for_insert, function(err){
                                if(err) throw err;
                                console.log("Node 1: 1 record inserted into fail_log");
                                closeConnection(con1);
                                throw errNode3;
                            });
                        } 
                        else{
                            console.log("Node 3: updated " + results.affectedRows + " records");
                            closeConnection(con3);
                            closeConnection(con1);
                        }
                    });
                };
            }
        });
    }
}
module.exports = {closeConnection, searchRecord, insertOneRecordIntoAllNodes};
