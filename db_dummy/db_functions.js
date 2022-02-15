// import {con2, con3} from "./db_connections";
// import {con1} from "./dblocal_conn.js";
const connections = require('./db_connections');
const con1 = connections.con1;
const con2 = connections.con2;
const con3 = connections.con3

// const con1 = require('./dblocal_conn');
const mysql = require('mysql')
const lostConn = 'PROTOCOL_CONNECTION_LOST';

const startLogNoId = "INSERT INTO new_recovery_log (type, name, year, `rank`, genre, director, old_name, old_year, old_genre, old_director) VALUES (?,?,?,?,?,?,?,?,?,?);";
const startLogWithId = "";
const startLogValuesNoId = ['START', 'startTxn', 0, 0, 'startTxn', 'startTxn', 'startTxn', 0, 'startTxn', 'startTxn'];
const insertLogWithId = "INSERT INTO new_recovery_log (transaction_id, type, name, year, `rank`, genre, director, old_name, old_year, old_genre, old_director) VALUES (?,?,?,?,?,?,?,?,?,?,?);";
function closeConnection(con) {
    con.end(function (err) {
        if (err) throw err;
        console.log("Closed connection");
    });
}


function newSearch(field, value, callback) {
    let queryNode1 = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE ?? = ?;";
    let queryNode2 = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_pre1980 WHERE ?? = ?;";
    let queryNode3 = "SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_post1980 WHERE ?? = ?;";

    let values = [(field), (value)];

    // search node 1
    con1.query("SET autocommit = 0", function (err1) {
        if (err1) {
            // check other nodes
            if (field == 'year') {
                if (value >= 1980) {
                    //node3
                    con3.query("SET autocommit = 0", function (err) {
                        if (err) throw err;
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
                                            con3.commit((err) => {
                                                if (err) {
                                                    con3.rollback((err) => {
                                                        if (err) throw err;
                                                        closeConnection(con3);
                                                    });
                                                } else {
                                                    // unlocking was successful
                                                    con3.query("UNLOCK TABLES", (err) => {
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
                    con2.query("SET autocommit = 0", function (err2) {
                        if (err2) throw err2;
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
                                            con2.commit((err) => {
                                                if (err) {
                                                    con2.rollback((err) => {
                                                        if (err) throw err;
                                                        closeConnection(con2);
                                                    });
                                                } else {
                                                    // unlocking successful
                                                    con2.query("UNLOCK TABLES", (err) => {
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
            } else {
                // field is not year
                con2.query("SET autocommit = 0", function (err2) {
                    if (err2) throw err2;
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
                                    } else if (res.length > 0) {
                                        // result is in node
                                        con2.commit((err) => {
                                            if (err) {
                                                con2.rollback((err) => {
                                                    if (err) throw err;
                                                    closeConnection(con2);
                                                });
                                            } else {
                                                // unlock successful
                                                con2.query("UNLOCK TABLES", (err) => {
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
                                            if (err) throw err;
                                            con2.query("UNLOCK TABLES", (err) =>{
                                                if (err) throw err;
                                                closeConnection(con2);
                                            })
                                        });
                                    };
                                });
                            };
                        });
                    };
                });

                con3.query("SET autocommit = 0", function (err3) {
                    if (err3) throw err;
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
                                    } else if (res.length > 0) {
                                        // is in this node
                                        con3.commit((err) => {
                                            if (err) {
                                                con3.rollback((err) => {
                                                    if (err) throw err;
                                                    closeConnection(con3);
                                                });
                                            } else {
                                                con3.query("UNLOCK TABLES", (err) => {
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
                                            con3.query("UNLOCK TABLES", (err) => {
                                                if(err) throw err;
                                                closeConnection(con3)
                                            })
                                            if (err) throw err;
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
                            con1.commit((err) => {
                                if (err) {
                                    con1.rollback((err) => {
                                        if (err) throw err;
                                        closeConnection(con1);
                                    });
                                } else {
                                    // unlock successful
                                    con1.query("UNLOCK TABLES", (err) => {
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

function generateAllReports(genre, year, director, callback){
    var answers = [];
    // Total Number of Movies
    con1.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_all;", function(err, result){
        if(err){
            throw err;
        }

        answers.push(result[0]['COUNT(DISTINCT name, year)']);

        // Number of [GENRE] Movies in [YEAR]
        con1.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_all WHERE genre=? AND year=?;", [genre, year], function(err, result){
            if(err){
                throw err;
            }
    
            answers.push(result[0]['COUNT(DISTINCT name, year)']);

            // Number of [GENRE] Movies in [YEAR] by [DIRECTOR]
            con1.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_all WHERE genre=? AND year=? AND director=?;", [genre, year, director], function(err, result){
                if(err){
                    throw err;
                }

                closeConnection(con1);
                answers.push(result[0]['COUNT(DISTINCT name, year)']);
                return callback(answers);
            });
        });
    });
}


function reallyNewInsert(name, year, rank, genre, director, callback) {
    let node1Sucess = false, node2Success = false, node3Success = false;
    con1.query("SET autocommit = 0", function (err1) {
        con1.query("LOCK TABLES new_recovery_log WRITE, final_movies_all WRITE", function (err1) {
            if (err1) {
                return con1.rollback(function () {
                    throw err1;
                });
            }

            con1.query(startLogNoId, startLogValuesNoId, function (err1, results) {
                if (err1) {
                    return con1.rollback(function () {
                        throw err1;
                    });
                }

                var txnId = results.insertId;
                con1.query(insertLogWithId, [txnId, 'INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err1) {
                    if (err1) {
                        return con1.rollback(function () {
                            throw err1;
                        });
                    }

                    if (err1) {
                        return con1.rollback(function () {
                            throw err1;
                        });
                    }

                    con1.query("INSERT INTO final_movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err1) {
                        if (err1) {
                            return con1.rollback(function () {
                                throw err1;
                            });
                        }

                        con1.query(insertLogWithId, [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn', 'commitTxn', 0, 'commitTxn', 'commitTxn'], function (err1) {
                            if (err1) {
                                return con1.rollback(function () {
                                    throw err1;
                                });
                            }

                            con1.commit(function (err1) {
                                if (err1) {
                                    return con1.rollback(function () {
                                        throw err1;
                                    });
                                }
                                con1.query("UNLOCK TABLES", function (err1) {
                                    if (err1) {
                                        return con1.rollback(function () {
                                            throw err1;
                                        });
                                    }
                                    node1Success = true;
                                    closeConnection(con1);
                                });
                            });
                        });
                    });
                });
            });
        });
    });

    if (year < 1980) {
        con2.query("SET autocommit = 0", function (err2) {
            if (err2) {
                return con2.rollback(function () {
                    throw err2;
                });
            }
            con2.query("LOCK TABLES new_recovery_log WRITE, final_movies_pre1980 WRITE", function (err2) {
                if (err2) {
                    return con2.rollback(function () {
                        throw err2;
                    });
                }

                con2.query(startLogNoId, startLogValuesNoId, function (err2, results) {
                    if (err2) {
                        return con2.rollback(function () {
                            throw err2;
                        });
                    }

                    txnId = results.insertId;
                    con2.query(insertLogWithId, [txnId, 'INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err2) {
                        if (err2) {
                            return con2.rollback(function () {
                                throw err2;
                            });
                        }

                        con2.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err2) {
                            if (err2) {
                                return con2.rollback(function () {
                                    throw err2;
                                });
                            }

                            con2.query(insertLogWithId, [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn', 'commitTxn', 0, 'commitTxn', 'commitTxn'], function (err2) {
                                if (err2) {
                                    return con2.rollback(function () {
                                        throw err2;
                                    });
                                }

                                con2.commit(function (err2) {
                                    if (err2) {
                                        return con2.rollback(function () {
                                            throw err2;
                                        });
                                    }
                                    con2.query("UNLOCK TABLES", function (err2) {
                                        if (err2) {
                                            return con2.rollback(function () {
                                                throw err2;
                                            });
                                        }
                                        node2Success = true;
                                        closeConnection(con2);
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });

    }

    if (year >= 1980) {
        con3.query("SET autocommit = 0", function (err3) {
            if (err3) {
                return con3.rollback(function () {
                    throw err3;
                });
            }

            con3.query("LOCK TABLE new_recovery_log WRITE, final_movies_post1980 WRITE", function (err3) {
                if (err3) {
                    return con3.rollback(function () {
                        throw err3;
                    });
                }

                con3.query(startLogNoId, startLogValuesNoId, function (err3, results) {
                    if (err3) {
                        return con3.rollback(function () {
                            throw err3;
                        });
                    }

                    txnId = results.insertId;
                    con3.query(insertLogWithId, [txnId, 'INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err3) {
                        if (err3) {
                            return con3.rollback(function () {
                                throw err3;
                            });
                        }

                        con3.query("INSERT INTO final_movies_post1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err3) {
                            if (err3) {
                                return con3.rollback(function () {
                                    throw err3;
                                });
                            }

                            con3.query(insertLogWithId, [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn', 'commitTxn', 0, 'commitTxn', 'commitTxn'], function (err3) {
                                if (err3) {
                                    return con3.rollback(function () {
                                        throw err3;
                                    });
                                }

                                con3.commit(function (err3) {
                                    if (err3) {
                                        return con3.rollback(function () {
                                            throw err3;
                                        });
                                    }
                                    con3.query("UNLOCK TABLES", function (err3) {
                                        if (err3) {
                                            return con3.rollback(function () {
                                                throw err3;
                                            });
                                        }
                                        node3Success = true;
                                        closeConnection(con3);
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    }
    if(node1Sucess && (node3Success || node2Success)){
        callback(true);
    }
}

function reallyNewUpdate(name, year, rank, genre, director, old_name, old_year, old_genre, old_director, callback) {
    // TODO: fix issue where commit log gets inserted before the update log (?)
    // TODO: modify recovery log to contain both old and new values (?)
    var oldYearIsPost1980 = old_year >= 1980;
    var newYearIsPost1980 = year >= 1980;
    var hasCalledback = false;
    if (oldYearIsPost1980 && !newYearIsPost1980) {
        con1.query("SET autocommit = 0", function (err1) {
            if (err1) {
                return con1.rollback(function () {
                    throw err1;
                });
            }
            con1.query("LOCK TABLE recovery_log WRITE, final_movies_all WRITE", function (err1) {
                if (err1) {
                    return con1.rollback(function () {
                        throw err1;
                    });
                }

                con1.query("INSERT INTO recovery_log (type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", ['START', 'startTxn', 0, 0, 'startTxn', 'startTxn'], function (err1, results) {
                    if (err1) {
                        return con1.rollback(function () {
                            throw err1;
                        });
                    }

                    var txnId = results.insertId;
                    con1.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'UPDATE', name, year, rank, genre, director], function (err1) {
                        if (err1) {
                            return con1.rollback(function () {
                                throw err1;
                            });
                        }

                        con1.query("UPDATE final_movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err1) {
                            if (err1) {
                                return con1.rollback(function () {
                                    throw err1;
                                });
                            }

                            con1.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn'], function (err1) {
                                if (err1) {
                                    return con1.rollback(function () {
                                        throw err1;
                                    });
                                }

                                con1.query("UNLOCK TABLES", function (err1) {
                                    if (err1) {
                                        return con1.rollback(function () {
                                            throw err1;
                                        });
                                    }

                                    con1.commit(function (err1) {
                                        if (err1) {
                                            return con1.rollback(function () {
                                                throw err1;
                                            });
                                        }
                                        callback(true);
                                        closeConnection(con1);
                                    });
                                });
                            });

                            con2.query("SET autocommit = 0", function (err2) {
                                if (err2) {
                                    return con2.rollback(function () {
                                        throw err2;
                                    });
                                }
                                con2.query("LOCK TABLE recovery_log WRITE, final_movies_pre1980 WRITE", function (err2) {
                                    if (err2) {
                                        return con2.rollback(function () {
                                            throw err2;
                                        });
                                    }

                                    con2.query("INSERT INTO recovery_log (type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", ['START', 'startTxn', 0, 0, 'startTxn', 'startTxn'], function (err2, results) {
                                        if (err2) {
                                            return con2.rollback(function () {
                                                throw err2;
                                            });
                                        }

                                        txnId = results.insertId;
                                        con2.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'INSERT', name, year, rank, genre, director], function (err2) {
                                            if (err2) {
                                                return con2.rollback(function () {
                                                    throw err2;
                                                });
                                            }

                                            con2.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err2) {
                                                if (err2) {
                                                    return con2.rollback(function () {
                                                        throw err2;
                                                    });
                                                }

                                                con2.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn'], function (err2) {
                                                    if (err2) {
                                                        return con2.rollback(function () {
                                                            throw err2;
                                                        });
                                                    }
                                                    con2.commit(function (err2) {
                                                        if (err2) {
                                                            return con2.rollback(function () {
                                                                throw err2;
                                                            });
                                                        }

                                                        con2.query("UNLOCK TABLES", function (err2) {
                                                            if (err2) {
                                                                return con2.rollback(function () {
                                                                    throw err2;
                                                                });
                                                            }
                                                            callback(true);
                                                            closeConnection(con2);
                                                        });
                                                    });
                                                });
                                            });
                                        });
                                    });
                                });
                            });

                            con3.query("SET autocommit = 0", function (err3) {
                                if (err3) {
                                    return con3.rollback(function () {
                                        throw err3;
                                    });
                                }

                                con3.query("LOCK TABLE recovery_log WRITE, final_movies_post1980 WRITE", function (err3) {
                                    if (err3) {
                                        return con3.rollback(function () {
                                            throw err3;
                                        });
                                    }

                                    con3.query("INSERT INTO recovery_log (type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", ['START', 'startTxn', 0, 0, 'startTxn', 'startTxn'], function (err3, results) {
                                        if (err3) {
                                            return con3.rollback(function () {
                                                throw err3;
                                            });
                                        }

                                        txnId = results.insertId;
                                        con3.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'DELETE', old_name, old_year, 0, old_genre, old_director], function (err3, results) {
                                            if (err3) {
                                                return con3.rollback(function () {
                                                    throw err3;
                                                });
                                            }

                                            txnId = results.insertId;

                                            con3.query("DELETE FROM final_movies_post1980 WHERE name=? AND year=? AND genre=? AND director=?", [old_name, old_year, old_genre, old_director], function (err3) {
                                                if (err3) {
                                                    return con3.rollback(function () {
                                                        throw err3;
                                                    });
                                                }

                                                con3.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn'], function (err3) {
                                                    if (err3) {
                                                        return con3.rollback(function () {
                                                            throw err3;
                                                        });
                                                    }
                                                    con3.commit(function (err3) {
                                                        if (err3) {
                                                            return con3.rollback(function () {
                                                                throw err3;
                                                            });
                                                        }

                                                        con3.query("UNLOCK TABLES", function (err3) {
                                                            if (err3) {
                                                                return con3.rollback(function () {
                                                                    throw err3;
                                                                });
                                                            }
                                                            callback(true);
                                                            closeConnection(con3);
                                                        });
                                                    })
                                                });
                                            });
                                        });
                                    });
                                });
                            });
                        });

                    });
                });
            });
        });
    }
    else if (!oldYearIsPost1980 && newYearIsPost1980) {
        con1.query("SET autocommit = 0", function (err1) {
            if (err1) {
                return con1.rollback(function () {
                    throw err1;
                });
            }
            con1.query("LOCK TABLE recovery_log WRITE, final_movies_all WRITE", function (err1) {
                if (err1) {
                    return con1.rollback(function () {
                        throw err1;
                    });
                }

                con1.query("INSERT INTO recovery_log (type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", ['START', 'startTxn', 0, 0, 'startTxn', 'startTxn'], function (err1, results) {
                    if (err1) {
                        return con1.rollback(function () {
                            throw err1;
                        });
                    }

                    var txnId = results.insertId;
                    con1.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'UPDATE', name, year, rank, genre, director], function (err1) {
                        if (err1) {
                            return con1.rollback(function () {
                                throw err1;
                            });
                        }

                        con1.query("UPDATE final_movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err1) {
                            if (err1) {
                                return con1.rollback(function () {
                                    throw err1;
                                });
                            }

                            con1.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn'], function (err1) {
                                if (err1) {
                                    return con1.rollback(function () {
                                        throw err1;
                                    });
                                }
                                con1.commit(function (err1) {
                                    if (err1) {
                                        return con1.rollback(function () {
                                            throw err1;
                                        });
                                    }

                                    con1.query("UNLOCK TABLES", function (err1) {
                                        if (err1) {
                                            return con1.rollback(function () {
                                                throw err1;
                                            });
                                        }
                                        if(!hasCalledback){
                                            callback(true);
                                            hasCalledback = true
                                        } 
                                        closeConnection(con1);
                                    });
                                });
                            });

                            con3.query("SET autocommit = 0", function (err3) {
                                if (err3) {
                                    return con3.rollback(function () {
                                        throw err3;
                                    });
                                }

                                con3.query("LOCK TABLE recovery_log WRITE, final_movies_post1980 WRITE", function (err3) {
                                    if (err3) {
                                        return con3.rollback(function () {
                                            throw err3;
                                        });
                                    }

                                    con3.query("INSERT INTO recovery_log (type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", ['START', 'startTxn', 0, 0, 'startTxn', 'startTxn'], function (err3, results) {
                                        if (err3) {
                                            return con3.rollback(function () {
                                                throw err3;
                                            });
                                        }

                                        txnId = results.insertId;
                                        con3.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'INSERT', name, year, rank, genre, director], function (err3) {
                                            if (err3) {
                                                return con3.rollback(function () {
                                                    throw err3;
                                                });
                                            }

                                            con3.query("INSERT INTO final_movies_post1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err3) {
                                                if (err3) {
                                                    return con3.rollback(function () {
                                                        throw err3;
                                                    });
                                                }

                                                con3.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn'], function (err3) {
                                                    if (err3) {
                                                        return con3.rollback(function () {
                                                            throw err3;
                                                        });
                                                    }
                                                    con3.commit(function (err3) {
                                                        if (err3) {
                                                            return con3.rollback(function () {
                                                                throw err3;
                                                            });
                                                        }
                                                        con3.query("UNLOCK TABLES", function (err3) {
                                                            if (err3) {
                                                                return con3.rollback(function () {
                                                                    throw err3;
                                                                });
                                                            }
                                                            if(!hasCalledback){
                                                                callback(true);
                                                                hasCalledback = true
                                                            } 
                                                            closeConnection(con3);
                                                        })
                                                    });
                                                });
                                            });
                                        });
                                    });
                                });
                            });
                        });

                        con2.query("SET autocommit = 0", function (err2) {
                            if (err2) {
                                return con2.rollback(function () {
                                    throw err2;
                                });
                            }
                            con2.query("LOCK TABLE recovery_log WRITE, final_movies_pre1980 WRITE", function (err2) {
                                if (err2) {
                                    return con2.rollback(function () {
                                        throw err2;
                                    });
                                }

                                con2.query("INSERT INTO recovery_log (type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?);", ['START', 'startTxn', 0, 0, 'startTxn', 'startTxn'], function (err2, results) {
                                    if (err2) {
                                        return con2.rollback(function () {
                                            throw err2;
                                        });
                                    }

                                    txnId = results.insertId;
                                    con2.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'DELETE', old_name, old_year, 0, old_genre, old_director], function (err2, results) {
                                        if (err2) {
                                            return con2.rollback(function () {
                                                throw err2;
                                            });
                                        }

                                        txnId = results.insertId;
                                        con2.query("DELETE FROM final_movies_pre1980 WHERE name=? AND year=? AND genre=? AND director=?", [old_name, old_year, old_genre, old_director], function (err2) {
                                            if (err2) {
                                                return con2.rollback(function () {
                                                    throw err2;
                                                });
                                            }

                                            con2.query("INSERT INTO recovery_log (transaction_id, type, name, year, `rank`, genre, director) VALUES (?,?,?,?,?,?,?);", [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn'], function (err2) {
                                                if (err2) {
                                                    return con2.rollback(function () {
                                                        throw err2;
                                                    });
                                                }

                                                con2.commit(function (err2) {
                                                    if (err2) {
                                                        return con2.rollback(function () {
                                                            throw err2;
                                                        });
                                                    }
                                                    con2.query("UNLOCK TABLES", function (err2) {
                                                        if (err2) {
                                                            return con2.rollback(function () {
                                                                throw err2;
                                                            });
                                                        }
                                                        if(!hasCalledback){
                                                            callback(true);
                                                            hasCalledback = true
                                                        } 
                                                        closeConnection(con2);
                                                    })
                                                });
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    }
    else {
        con1.query("SET autocommit = 0", function (err1) {
            if (err1) {
                return con1.rollback(function () {
                    throw err1;
                });
            }

            con1.query("LOCK TABLES new_recovery_log WRITE, final_movies_all WRITE", function (err1) {
                if (err1) {
                    return con1.rollback(function () {
                        throw err1;
                    });
                }

                con1.query(startLogNoId, startLogValuesNoId, function (err1, results) {
                    if (err1) {
                        return con1.rollback(function () {
                            throw err1;
                        });
                    }

                    var txnId = results.insertId;
                    con1.query(insertLogWithId, [txnId, 'UPDATE', name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err1) {
                        if (err1) {
                            return con1.rollback(function () {
                                throw err1;
                            });
                        }

                        con1.query("UPDATE final_movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err1) {
                            if (err1) {
                                return con1.rollback(function () {
                                    throw err1;
                                });
                            }

                            con1.query(insertLogWithId, [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn', 'commitTxn', 0, 'commitTxn', 'commitTxn'], function (err1) {
                                if (err1) {
                                    return con1.rollback(function () {
                                        throw err1;
                                    });
                                }
                                con1.commit(function (err1) {
                                    if (err1) {
                                        return con1.rollback(function () {
                                            throw err1;
                                        });
                                    }
                                    con1.query("UNLOCK TABLES", function (err1) {
                                        if (err1) {
                                            return con1.rollback(function () {
                                                throw err1;
                                            });
                                        }
                                        if(!hasCalledback){
                                            callback(true);
                                            hasCalledback = true
                                        } 
                                        // closeConnection(con1);
                                    });
                                });
                            });
                        });
                    })
                });
            });
        });

        if (year < 1980) {
            con2.query("SET autocommit = 0", function (err2) {
                if (err2) {
                    return con2.rollback(function () {
                        throw err2;
                    });
                }
                con2.query("LOCK TABLES new_recovery_log WRITE, final_movies_pre1980 WRITE", function (err2) {
                    if (err2) {
                        return con2.rollback(function () {
                            throw err2;
                        });
                    }

                    con2.query(startLogNoId, startLogValuesNoId, function (err2, results) {
                        if (err2) {
                            return con2.rollback(function () {
                                throw err2;
                            });
                        }

                        txnId = results.insertId;
                        con2.query(insertLogWithId, [txnId, 'UPDATE', name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err2) {
                            if (err2) {
                                return con2.rollback(function () {
                                    throw err2;
                                });
                            }

                            con2.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err2) {
                                if (err2) {
                                    return con2.rollback(function () {
                                        throw err2;
                                    });
                                }

                                con2.query(insertLogWithId, [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn', 'commitTxn', 0, 'commitTxn', 'commitTxn'], function (err2) {
                                    if (err2) {
                                        return con2.rollback(function () {
                                            throw err2;
                                        });
                                    }
                                    con2.commit(function (err2) {
                                        if (err2) {
                                            return con2.rollback(function () {
                                                throw err2;
                                            });
                                        }
                                        con2.query("UNLOCK TABLES", function (err2) {
                                            if (err2) {
                                                return con2.rollback(function () {
                                                    throw err2;
                                                });
                                            }
                                            if(!hasCalledback){
                                                callback(true);
                                                hasCalledback = true
                                            } 
                                            // closeConnection(con2);
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        }

        if (year >= 1980) {
            con3.query("SET autocommit = 0", function (err3) {
                if (err3) {
                    return con3.rollback(function () {
                        throw err3;
                    });
                }
                con3.query("LOCK TABLES new_recovery_log WRITE, final_movies_post1980 WRITE", function (err3) {
                    if (err3) {
                        return con3.rollback(function () {
                            throw err3;
                        });
                    }
                    con3.query(startLogNoId, startLogValuesNoId, function (err3, results) {
                        if (err3) {
                            return con3.rollback(function () {
                                throw err3;
                            });
                        }

                        txnId = results.insertId;
                        con3.query(insertLogWithId, [txnId, 'UPDATE', name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err3) {
                            if (err3) {
                                return con3.rollback(function () {
                                    throw err3;
                                });
                            }

                            con3.query("UPDATE final_movies_post1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err3) {
                                if (err3) {
                                    return con3.rollback(function () {
                                        throw err3;
                                    });
                                }

                                con3.query(insertLogWithId, [txnId, 'COMMIT', 'commitTxn', 0, 0, 'commitTxn', 'commitTxn', 'commitTxn', 0, 'commitTxn', 'commitTxn'], function (err3) {
                                    if (err3) {
                                        return con3.rollback(function () {
                                            throw err3;
                                        });
                                    }
                                    con3.commit(function (err3) {
                                        if (err3) {
                                            return con3.rollback(function () {
                                                throw err3;
                                            });
                                        }
                                        con3.query("UNLOCK TABLES", function (err3) {
                                            if (err3) {
                                                return con3.rollback(function () {
                                                    throw err3;
                                                });
                                            }
                                            if(!hasCalledback){
                                                callback(true);
                                                hasCalledback = true
                                            } 
                                            // closeConnection(con3);
                                        })
                                    })
                                });
                            });
                        });
                    });
                });
            });
        }
    }
}

function recover(){

    //TODO:check if servers are online before attempting recovery
    //TODO:error handling

    con1.query("LOCK TABLE recovery_log WRITE, final_movies_all WRITE", function (err1) {
        con2.query("LOCK TABLE recovery_log WRITE, final_movies_pre1980 WRITE", function (err1) {
            con3.query("LOCK TABLE recovery_log WRITE, final_movies_pre1980 WRITE", function (err1) {

                //read con1 logs
                con1.query("SELECT * from new_recovery_log;", values, function (err, results) {

                    results.foreach(resultitem => {
        
                        var type = resultitem.type;
                        var name = resultitem.name;
                        var year = resultitem.year;
                        var rank = resultitem.rank;
                        var genre = resultitem.genre;
                        var director = resultitem.director;
        
                        var old_name = resultitem.name;
                        var old_year = resultitem.year;
                        var old_genre = resultitem.genre;
                        var old_director = resultitem.director;
        
                        if(resultitem.type == "INSERT" || resultitem.type == "UPDATE"){
        
                            if(resultitem.year < 1980){
                                con2.query("SELECT * from new_recovery_log WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err, results){
                                    if(results.length == 0){ //search for corresponding log entry in other node, if not found perform query
        
                                        if(type == "INSERT"){
                                            con2.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                                con2.commit(function (err) {})
                                            });
        
                                        }
                                        else if(type == "UPDATE"){
                                            con2.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                                con2.commit(function (err) {})
                                            });
                                        }

                                        con1.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function(err){
                                            con1.commit(function (err3) {})
                                        }); //remove from log

                                    }else{
                                        con1.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function(err){
                                            con1.commit(function (err) {})
                                        });

                                        con2.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function(err){
                                            con1.commit(function (err) {})
                                        });
                                    }
                                })
                            }
                            else if(resultitem.year >= 1980){
                                con3.query("SELECT * from new_recovery_log WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err, results){
                                    if(results.length == 0){
        
                                        if(type == "INSERT"){
                                            con3.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                                con3.commit(function (err) {})
                                            });
        
                                        }
                                        else if(type == "UPDATE"){
                                            con3.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                                con3.commit(function (err) {})
                                            });
                                        }

                                        con1.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function(err){
                                            con1.commit(function (err3) {})
                                        });

                                    }else{
                                        con1.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function(err){
                                            con1.commit(function (err) {})
                                        });

                                        con3.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function(err){
                                            con3.commit(function (err) {})
                                        });
                                    }
                                })
                            }
                        }
                        else{
                            con1.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;",function (err, result) {
                                con1.commit(function (err) {})
                            });
                        }
                    });
                
                })

                //read con2 logs
                con2.query("SELECT * from new_recovery_log;", values, function (err, results) {

                    results.foreach(resultitem => {
        
                        var type = resultitem.type;
                        var name = resultitem.name;
                        var year = resultitem.year;
                        var rank = resultitem.rank;
                        var genre = resultitem.genre;
                        var director = resultitem.director;
        
                        var old_name = resultitem.name;
                        var old_year = resultitem.year;
                        var old_genre = resultitem.genre;
                        var old_director = resultitem.director;

                        //remaining insert/update logs are entries that were
                        //missed/were not present in Node 1 
                        //no need to check for corresponding entry
        
                        if(type == "INSERT"){
                            con1.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                con1.commit(function (err) {})
                            });
                        }
                        else if(type == "UPDATE"){
                            con1.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                con1.commit(function (err) {})
                            });
                        }

                        con2.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;",function (err, result) {
                            con2.commit(function (err) {})
                        });
                    });
                })

                //read con3 logs
                con3.query("SELECT * from new_recovery_log;", values, function (err, results) {

                    results.foreach(resultitem => {
        
                        var type = resultitem.type;
                        var name = resultitem.name;
                        var year = resultitem.year;
                        var rank = resultitem.rank;
                        var genre = resultitem.genre;
                        var director = resultitem.director;
        
                        var old_name = resultitem.name;
                        var old_year = resultitem.year;
                        var old_genre = resultitem.genre;
                        var old_director = resultitem.director;
        
                        if(type == "INSERT"){
                            con1.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                con1.commit(function (err) {})
                            });
                        }
                        else if(type == "UPDATE"){
                            con1.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                con1.commit(function (err) {})
                            });
                        }

                        con3.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;",function (err, result) {
                            con3.commit(function (err) {})
                        });
                    });
                })
                
                con1.query("UNLOCK TABLES", function (err3) {
                    if (err) throw err;
                    closeConnection(con1);
                });

                con2.query("UNLOCK TABLES", function (err3) {
                    if (err) throw err;
                    closeConnection(con2);
                });

                con3.query("UNLOCK TABLES", function (err3) {
                    if (err) throw err;
                    closeConnection(con3);
                });
            })
        })
    })
}
module.exports = { newSearch, reallyNewUpdate, reallyNewInsert };
// module.exports = { closeConnection, searchRecord, insertOneRecordIntoAllNodes };
