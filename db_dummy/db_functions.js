// import {con2, con3} from "./db_connections";
// import {con1} from "./dblocal_conn.js";
const connections = require('./db_connections');
let con1, con2, con3;
con1 = connections.createConnectionNode1();
con2 = connections.createConnectionNode2();
con3 = connections.createConnectionNode3();
// const con1 = require('./dblocal_conn');
const mysql = require('mysql');
const lostConn = 'PROTOCOL_CONNECTION_LOST';

const startLogNoId = "INSERT INTO new_recovery_log (type, name, year, `rank`, genre, director, old_name, old_year, old_genre, old_director) VALUES (?,?,?,?,?,?,?,?,?,?);";
const startLogWithId = "";
const startLogValuesNoId = ['START', 'startTxn', 0, 0, 'startTxn', 'startTxn', 'startTxn', 0, 'startTxn', 'startTxn'];
const insertLogWithId = "INSERT INTO new_recovery_log (transaction_id, type, name, year, `rank`, genre, director, old_name, old_year, old_genre, old_director) VALUES (?,?,?,?,?,?,?,?,?,?,?);";
const insertLogNoId = "INSERT INTO new_recovery_log (type, name, year, `rank`, genre, director, old_name, old_year, old_genre, old_director) VALUES (?,?,?,?,?,?,?,?,?,?);";
function closeConnection(con) {
    con.end(function (err) {
        if (err) throw err;
        console.log("Closed connection " + con.config.database);
    });
}


function newSearch(field, value, callback) {
    let queryNode1 = "SELECT `name`, `year`, `rank`, genre, director FROM final_movies_all WHERE ?? = ?;";
    let queryNode2 = "SELECT `name`, `year`, `rank`, genre, director FROM final_movies_pre1980 WHERE ?? = ?;";
    let queryNode3 = "SELECT `name`, `year`, `rank`, genre, director FROM final_movies_post1980 WHERE ?? = ?;";

    let values = [(field), (value)];
    // con1 = connections.createConnectionNode1();
    // con2 = connections.createConnectionNode2();
    // con3 = connections.createConnectionNode3();
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
                            con3.query("LOCK TABLES final_movies_post1980 READ", (err) => {
                                if (err) throw err;
                                else {
                                    // locking was successful
                                    con3.query(queryNode3, values, (err, res) => {
                                        if (err) {
                                            con3.rollback((err) => {
                                                if (err) throw err;
                                                //closeconnection(con3);
                                            });
                                        } else {
                                            // query was successful
                                            con3.commit((err) => {
                                                if (err) {
                                                    con3.rollback((err) => {
                                                        if (err) throw err;
                                                        //closeconnection(con3);
                                                    });
                                                } else {
                                                    // unlocking was successful
                                                    con3.query("UNLOCK TABLES", (err) => {
                                                        if (err) con3.rollback((err) => {
                                                            if (err) throw err;
                                                            //closeconnection(con3);
                                                        });
                                                        // commit was successful
                                                        //closeconnection(con3);
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
                            con2.query("LOCK TABLES final_movies_pre1980 READ", (err) => {
                                if (err) throw err;
                                else {
                                    // locking successful
                                    con2.query(queryNode2, values, (err, res) => {
                                        if (err) {
                                            con2.rollback((err) => {
                                                if (err) throw err;
                                                //closeconnection(con2);
                                            });
                                        } else {
                                            // query successful
                                            con2.commit((err) => {
                                                if (err) {
                                                    con2.rollback((err) => {
                                                        if (err) throw err;
                                                        //closeconnection(con2);
                                                    });
                                                } else {
                                                    // unlocking successful
                                                    con2.query("UNLOCK TABLES", (err) => {
                                                        if (err) con2.rollback((err) => {
                                                            if (err) throw err;
                                                        });
                                                        //closeconnection(con2);
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
                        con2.query("LOCK TABLES final_movies_pre1980 READ", (err) => {
                            if (err) throw err;
                            else {
                                // locking successful
                                con2.query(queryNode2, values, (err, res) => {
                                    if (err) {
                                        con2.rollback((err) => {
                                            if (err) throw err;
                                            //closeconnection(con2);
                                        });
                                    } else if (res.length > 0) {
                                        // result is in node
                                        con2.commit((err) => {
                                            if (err) {
                                                con2.rollback((err) => {
                                                    if (err) throw err;
                                                    //closeconnection(con2);
                                                });
                                            } else {
                                                // unlock successful
                                                con2.query("UNLOCK TABLES", (err) => {
                                                    if (err) con2.rollback((err) => {
                                                        if (err) throw err;
                                                    });
                                                    //closeconnection(con2)
                                                    return callback(res);
                                                });
                                            };
                                        });
                                    } else {
                                        // not in this node
                                        con2.rollback((err) => {
                                            if (err) throw err;
                                            con2.query("UNLOCK TABLES", (err) => {
                                                if (err) throw err;
                                                //closeconnection(con2);
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
                        con3.query("LOCK TABLES final_movies_post1980 READ", (err) => {
                            if (err) throw err;
                            else {
                                // locking succesful
                                con3.query(queryNode3, values, (err, res) => {
                                    if (err) {
                                        con3.rollback((err) => {
                                            if (err) throw err;
                                            //closeconnection(con3);
                                        });
                                    } else if (res.length > 0) {
                                        // is in this node
                                        con3.commit((err) => {
                                            if (err) {
                                                con3.rollback((err) => {
                                                    if (err) throw err;
                                                    //closeconnection(con3);
                                                });
                                            } else {
                                                con3.query("UNLOCK TABLES", (err) => {
                                                    if (err) con3.rollback((err) => {
                                                        if (err) throw err;
                                                    });
                                                    //closeconnection(con3);
                                                    return callback(res);
                                                });
                                            };
                                        });
                                    } else {
                                        // not in this node
                                        con3.rollback((err) => {
                                            con3.query("UNLOCK TABLES", (err) => {
                                                if (err) throw err;
                                                //closeconnection(con3)
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
            con1.query("LOCK TABLES final_movies_all READ", (err) => {
                if (err) {
                    con1.rollback((err) => {
                        if (err) throw err;
                        //closeconnection(con1);
                    });
                } else {
                    // locking successful
                    con1.query(queryNode1, values, (err, res) => {
                        if (err) {
                            con1.rollback((err) => {
                                if (err) throw err;
                                //closeconnection(con1);
                            });
                        } else {
                            // query successful
                            con1.commit((err) => {
                                if (err) {
                                    con1.rollback((err) => {
                                        if (err) throw err;
                                        //closeconnection(con1);
                                    });
                                } else {
                                    // unlock successful
                                    con1.query("UNLOCK TABLES", (err) => {
                                        if (err) throw err;
                                        // commit successful
                                        //closeconnection(con1);
                                        console.log(res);
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

function generateAllReports(genre, year, director, callback) {
    var answers = [];
    // con1 = connections.createConnectionNode1();
    // con2 = connections.createConnectionNode2();
    // con3 = connections.createConnectionNode3();

    // Total Number of Movies
    con1.query("SET autocommit = 0", function (err) {
        if (err) {
            if(year < 1980){ // node 2
                console.log("here");
                con2.query("LOCK TABLES final_movies_pre1980 READ", function (err) {
                    if (err) throw err;
    
                    con2.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_pre1980;", function (err, result) {
                        if (err) {
                            throw err;
                        }
    
                        answers.push(result[0]['COUNT(DISTINCT name, year)']);
    
                        // Number of [GENRE] Movies in [YEAR]
                        con2.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_pre1980 WHERE genre=? AND year=?;", [genre, year], function (err, result) {
                            if (err) {
                                throw err;
                            }
    
                            answers.push(result[0]['COUNT(DISTINCT name, year)']);
    
                            // Number of [GENRE] Movies in [YEAR] by [DIRECTOR]
                            con2.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_pre1980 WHERE genre=? AND year=? AND director=?;", [genre, year, director], function (err, result) {
                                if (err) {
                                    throw err;
                                }
                                    con3.commit((err)=> {
                                        if (err) throw err
                                        //closeconnection(con3);
                                        con3.query("UNLOCK TABLES", (err)=> {
                                            if (err) throw err
                                            answers.push(result[0]['COUNT(DISTINCT name, year)']);
                                            return callback(answers);
                                        })
                                    })
    
                            });
                        });
                    });
                });   
            }else{ //node 3
                con3.query("LOCK TABLES final_movies_post1980 READ", function (err) {
                    if (err) throw err;
    
                    con3.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_post1980;", function (err, result) {
                        if (err) {
                            throw err;
                        }
    
                        answers.push(result[0]['COUNT(DISTINCT name, year)']);
    
                        // Number of [GENRE] Movies in [YEAR]
                        con3.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_post1980 WHERE genre=? AND year=?;", [genre, year], function (err, result) {
                            if (err) {
                                throw err;
                            }
    
                            answers.push(result[0]['COUNT(DISTINCT name, year)']);
    
                            // Number of [GENRE] Movies in [YEAR] by [DIRECTOR]
                            con3.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_post1980 WHERE genre=? AND year=? AND director=?;", [genre, year, director], function (err, result) {
                                if (err) {
                                    throw err;
                                }
                                    con3.commit((err)=> {
                                        if (err) throw err
                                        //closeconnection(con3);
                                        con3.query("UNLOCK TABLES", (err)=> {
                                            if (err) throw err
                                            answers.push(result[0]['COUNT(DISTINCT name, year)']);
                                            return callback(answers);
                                        })
                                    })
    
                            });
                        });
                    });
                });
            }
        }else{
            con1.query("LOCK TABLES final_movies_all READ", function (err) {
                if (err) throw err;

                con1.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_all;", function (err, result) {
                    if (err) {
                        throw err;
                    }

                    answers.push(result[0]['COUNT(DISTINCT name, year)']);

                    // Number of [GENRE] Movies in [YEAR]
                    con1.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_all WHERE genre=? AND year=?;", [genre, year], function (err, result) {
                        if (err) {
                            throw err;
                        }

                        answers.push(result[0]['COUNT(DISTINCT name, year)']);

                        // Number of [GENRE] Movies in [YEAR] by [DIRECTOR]
                        con1.query("SELECT COUNT(DISTINCT name, year) FROM final_movies_all WHERE genre=? AND year=? AND director=?;", [genre, year, director], function (err, result) {
                            if (err) {
                                throw err;
                            }
                                con1.commit((err)=> {
                                    if (err) throw err
                                    //closeconnection(con1);
                                    con1.query("UNLOCK TABLES", (err)=> {
                                        if (err) throw err
                                        answers.push(result[0]['COUNT(DISTINCT name, year)']);
                                        return callback(answers);
                                    })
                                })

                        });
                    });
                });
            });
        }
    });
}


function reallyNewInsert(name, year, rank, genre, director, callback) {
    let hasCalledback = false;

    con1.query("SET autocommit = 0", function (err1) {
        con1.query("LOCK TABLES new_recovery_log WRITE, final_movies_all WRITE", function (err1) {
            if (err1) {
                return con1.rollback(function () {
                    throw err1;
                });
            }

            con1.query(insertLogNoId, ['INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err1) {
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
                            if (!hasCalledback) {
                                callback(true);
                                hasCalledback = true;
                            }
                            console.log("node 1 tables unlocked");
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

                con2.query(insertLogNoId, ['INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err2) {
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
                                if (!hasCalledback) {
                                    callback(true)
                                    hasCalledback = true;
                                }
                                console.log("node 2 tables unlocked")
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

                con3.query(insertLogNoId, ['INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err3) {
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
                                if (!hasCalledback) {
                                    callback(true)
                                    hasCalledback = true;
                                }
                                console.log("node 3 tables unlocked")
                            });
                        });
                    });
                });
            });
        });
    }
}

function reallyNewUpdate(name, year, rank, genre, director, old_name, old_year, old_genre, old_director, callback) {
    var hasCalledback = false;

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

            con1.query(insertLogNoId, ['UPDATE', name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err1) {
                if (err1) {
                    return con1.rollback(function () {
                        throw err1;
                    });
                }

                console.log("inserted to node 1 log");
                con1.query("UPDATE final_movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err1) {
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
                            if (!hasCalledback) {
                                callback(true);
                                hasCalledback = true
                            }
                            console.log("tables unlocked node 1");
                        });
                    });
                });
            })
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

                con2.query(insertLogNoId, ['UPDATE', name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err2) {
                    if (err2) {
                        return con2.rollback(function () {
                            throw err2;
                        });
                    }

                    console.log("inserted to node 2 log");
                    con2.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err2) {
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
                                if (!hasCalledback) {
                                    callback(true);
                                    hasCalledback = true
                                }
                                console.log("tables unlocked node 2");
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
                con3.query(insertLogNoId, ['UPDATE', name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err3) {
                    if (err3) {
                        return con3.rollback(function () {
                            throw err3;
                        });
                    }

                    console.log("inserted to node 3 log");
                    con3.query("UPDATE final_movies_post1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err3) {
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
                                if (!hasCalledback) {
                                    callback(true);
                                    hasCalledback = true
                                }
                                console.log("tables unlocked node 3");
                            })
                        })
                    });
                });
            });
        });
    }
}

function InsertSimulateReplicaError(name, year, rank, genre, director, callback) {
    let hasCalledback = false;

    con1.query("SET autocommit = 0", function (err1) {
        con1.query("LOCK TABLES new_recovery_log WRITE, final_movies_all WRITE", function (err1) {
            if (err1) {
                return con1.rollback(function () {
                    throw err1;
                });
            }

            con1.query(insertLogNoId, ['INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err1) {
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
                            if (!hasCalledback) {
                                callback(true);
                                hasCalledback = true;
                            }
                            //console.log("node 1 tables unlocked");
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

                con2.query(insertLogNoId, ['INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err2) {
                    if (err2) {
                        return con2.rollback(function () {
                            throw err2;
                        });
                    }

                    con2.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err2) {
                        if (true) {
                            return con2.rollback(function () {
                                con2.query("UNLOCK TABLES", function (err2) {
                                    if (err2) {
                                        return con2.rollback(function () {
                                            throw err2;
                                        });
                                    }
                                    if (!hasCalledback) {
                                        callback(true)
                                        hasCalledback = true;
                                    }
                                    //console.log("node 2 tables unlocked")
                                });
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
                                if (!hasCalledback) {
                                    callback(true)
                                    hasCalledback = true;
                                }
                                //console.log("node 2 tables unlocked")
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

                con3.query(insertLogNoId, ['INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err3) {
                    if (err3) {
                        return con3.rollback(function () {
                            throw err3;
                        });
                    }

                    con3.query("INSERT INTO final_movies_post1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err3) {
                        if (true) {
                            return con3.rollback(function () {
                                con3.query("UNLOCK TABLES", function (err3) {
                                    if (err3) {
                                        return con3.rollback(function () {
                                            throw err3;
                                        });
                                    }
                                    if (!hasCalledback) {
                                        callback(true)
                                        hasCalledback = true;
                                    }
                                    //console.log("node 3 tables unlocked")
                                });
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
                                if (!hasCalledback) {
                                    callback(true)
                                    hasCalledback = true;
                                }
                                //console.log("node 3 tables unlocked")
                            });
                        });
                    });
                });
            });
        });
    }
}

function InsertSimulatePrimaryError(name, year, rank, genre, director, callback) {
    let hasCalledback = false;

    con1.query("SET autocommit = 0", function (err1) {
        con1.query("LOCK TABLES new_recovery_log WRITE, final_movies_all WRITE", function (err1) {
            if (err1) {
                return con1.rollback(function () {
                    throw err1;
                });
            }

            con1.query(insertLogNoId, ['INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err1) {
                if (err1) {
                    return con1.rollback(function () {
                        throw err1;
                    });
                }

                con1.query("INSERT INTO final_movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err1) {
                    if (true) {
                        return con1.rollback(function () {

                            con1.query("UNLOCK TABLES", function (err1) {
                                if (err1) {x
                                    return con1.rollback(function () {
                                        throw err1;
                                    });
                                }
                                if (!hasCalledback) {
                                    callback(true);
                                    hasCalledback = true;
                                }
                                //console.log("node 1 tables unlocked");
                            });

                        });
                    }

                    con1.commit(function (err1) {
                        if (err1) {
                            return con1.rollback(function () {
                                throw err1;
                            });
                        }
                        con1.query("UNLOCK TABLES", function (err1) {
                            if (err1) {x
                                return con1.rollback(function () {
                                    throw err1;
                                });
                            }
                            if (!hasCalledback) {
                                callback(true);
                                hasCalledback = true;
                            }
                            console.log("node 1 tables unlocked");
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

                con2.query(insertLogNoId, ['INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err2) {
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
                                if (!hasCalledback) {
                                    callback(true)
                                    hasCalledback = true;
                                }
                                //console.log("node 2 tables unlocked")
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

                con3.query(insertLogNoId, ['INSERT', name, year, rank, genre, director, "N/A", 0, "N/A", "N/A"], function (err3) {
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
                                if (!hasCalledback) {
                                    callback(true)
                                    hasCalledback = true;
                                }
                                //console.log("node 3 tables unlocked")
                            });
                        });
                    });
                });
            });
        });
    }
}

function recover() {

    //TODO:check if servers are online before attempting recovery
    //TODO:error handling

    // con1 = connections.createConnectionNode1();
    // con2 = connections.createConnectionNode2();
    // con3 = connections.createConnectionNode3();

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

                        if (resultitem.type == "INSERT" || resultitem.type == "UPDATE") {

                            if (resultitem.year < 1980) {
                                con2.query("SELECT * from new_recovery_log WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function (err, results) {
                                    if (results.length == 0) { //search for corresponding log entry in other node, if not found perform query

                                        if (type == "INSERT") {
                                            con2.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                                con2.commit(function (err) { })
                                            });

                                        }
                                        else if (type == "UPDATE") {
                                            con2.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                                con2.commit(function (err) { })
                                            });
                                        }

                                        con1.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function (err) {
                                            con1.commit(function (err3) { })
                                        }); //remove from log

                                    } else {
                                        con1.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function (err) {
                                            con1.commit(function (err) { })
                                        });

                                        con2.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function (err) {
                                            con1.commit(function (err) { })
                                        });
                                    }
                                })
                            }
                            else if (resultitem.year >= 1980) {
                                con3.query("SELECT * from new_recovery_log WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function (err, results) {
                                    if (results.length == 0) {

                                        if (type == "INSERT") {
                                            con3.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                                con3.commit(function (err) { })
                                            });

                                        }
                                        else if (type == "UPDATE") {
                                            con3.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                                con3.commit(function (err) { })
                                            });
                                        }

                                        con1.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function (err) {
                                            con1.commit(function (err3) { })
                                        });

                                    } else {
                                        con1.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function (err) {
                                            con1.commit(function (err) { })
                                        });

                                        con3.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function (err) {
                                            con3.commit(function (err) { })
                                        });
                                    }
                                })
                            }
                        }
                        else {
                            con1.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function (err, result) {
                                con1.commit(function (err) { })
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

                        if (type == "INSERT") {
                            con1.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                con1.commit(function (err) { })
                            });
                        }
                        else if (type == "UPDATE") {
                            con1.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                con1.commit(function (err) { })
                            });
                        }

                        con2.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function (err, result) {
                            con2.commit(function (err) { })
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

                        if (type == "INSERT") {
                            con1.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                con1.commit(function (err) { })
                            });
                        }
                        else if (type == "UPDATE") {
                            con1.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                con1.commit(function (err) { })
                            });
                        }

                        con3.query("DELETE FROM new_recovery_log WHERE HERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", function (err, result) {
                            con3.commit(function (err) { })
                        });
                    });
                })

                con1.query("UNLOCK TABLES", function (err3) {
                    if (err) throw err;
                    //closeconnection(con1);
                });

                con2.query("UNLOCK TABLES", function (err3) {
                    if (err) throw err;
                    //closeconnection(con2);
                });

                con3.query("UNLOCK TABLES", function (err3) {
                    if (err) throw err;
                    //closeconnection(con3);
                });
            })
        })
    })
}

function newRecover(){
    //read con1 logs
    con1.query("SELECT * from new_recovery_log;", function (err, results) {

        results.forEach(resultitem => {

            var txnId = resultitem.transaction_id;
            var type = resultitem.type;
            var name = resultitem.name;
            var year = resultitem.year;
            var rank = resultitem.rank;
            var genre = resultitem.genre;
            var director = resultitem.director;

            var old_name = resultitem.old_name;
            var old_year = resultitem.old_year;
            var old_genre = resultitem.old_genre;
            var old_director = resultitem.old_director;

            if(resultitem.type == "INSERT" || resultitem.type == "UPDATE"){
                if(resultitem.year < 1980){
                    con2.query("SELECT * from new_recovery_log WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err, results){
                        if(results.length == 0){ //search for corresponding log entry in other node, if not found perform query

                            if(type == "INSERT"){
                                con2.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                    con2.commit(function (err) {
                                        con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                            con1.commit(function (err3) {
                                            })
                                        }); //remove from log
                                    })
                                });

                            }
                            else if(type == "UPDATE"){
                                con2.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                    con2.commit(function (err) {
                                        con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                            con1.commit(function (err3) {
                                            })
                                        }); //remove from log
                                    })
                                });
                            }
                        }else{
                            con1.query("DELETE FROM new_recovery_log WHERE transaction_id=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [txnId, name, year, rank, genre, director], function(err){
                                con1.commit(function (err) {})
                            });

                            con2.query("DELETE FROM new_recovery_log WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err){
                                con1.commit(function (err) {})
                            });
                        }
                    })
                }
                else if(resultitem.year >= 1980){
                    con3.query("SELECT * from new_recovery_log WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err, results){
                        if(results.length == 0){
                            if(type == "INSERT"){
                                con3.query("INSERT INTO final_movies_post1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                    con3.commit(function (err) {
                                        con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                            con1.commit(function (err3) {})
                                        });
                                    })
                                });
                            }
                            else if(type == "UPDATE"){
                                con3.query("UPDATE final_movies_post1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                    con3.commit(function (err) {
                                        con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                            con1.commit(function (err3) {})
                                        });
                                    })
                                });
                            }
                        }else{
                            con1.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err){
                                con1.commit(function (err) {})
                            });

                            con3.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err){
                                con3.commit(function (err) {})
                            });
                        }
                    })
                }
            }
        });
    
    })

    // //read con2 logs
    // con2.query("SELECT * from new_recovery_log;", function (err, results) {
    //     results.forEach(resultitem => {

    //         var type = resultitem.type;
    //         var name = resultitem.name;
    //         var year = resultitem.year;
    //         var rank = resultitem.rank;
    //         var genre = resultitem.genre;
    //         var director = resultitem.director;

    //         var old_name = resultitem.name;
    //         var old_year = resultitem.year;
    //         var old_genre = resultitem.genre;
    //         var old_director = resultitem.director;

    //         //remaining insert/update logs are entries that were
    //         //missed/were not present in Node 1 
    //         //no need to check for corresponding entry

    //         if(type == "INSERT"){
    //             con1.query("INSERT INTO final_movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
    //                 con1.commit(function (err) {})
    //             });
    //         }
    //         else if(type == "UPDATE"){
    //             con1.query("UPDATE final_movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
    //                 con1.commit(function (err) {})
    //             });
    //         }

    //         con2.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function (err, result) {
    //             con2.commit(function (err) {})
    //         });
    //     });
    // })

    //read con3 logs
    con3.query("SELECT * from new_recovery_log;", function (err, results) {

        results.forEach(resultitem => {
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
                con1.query("INSERT INTO final_movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                    con1.commit(function (err) {
                        con3.query("DELETE FROM new_recovery_log WHERE HERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director],function (err, result) {
                            con3.commit(function (err) {})
                        });
                    })
                });
            }
            else if(type == "UPDATE"){
                con1.query("UPDATE final_movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                    con1.commit(function (err) {
                        con3.query("DELETE FROM new_recovery_log WHERE HERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director],function (err, result) {
                            con3.commit(function (err) {})
                        });
                    })
                });
            }
        });
    })

    // con1.query("LOCK TABLE recovery_log WRITE, final_movies_all WRITE", function (err1) {
    //     con2.query("LOCK TABLE recovery_log WRITE, final_movies_pre1980 WRITE", function (err2) {
    //         con3.query("LOCK TABLE recovery_log WRITE, final_movies_pre1980 WRITE", function (err3) {


                
    //             con1.query("UNLOCK TABLES", function (err1) {
    //                 if (err1) throw err1;
    //                 closeConnection(con1);
    //             });

    //             con2.query("UNLOCK TABLES", function (err2) {
    //                 if (err2) throw err2;
    //                 closeConnection(con2);
    //             });

    //             con3.query("UNLOCK TABLES", function (err3) {
    //                 if (err3) throw err3;
    //                 closeConnection(con3);
    //             });
    //         })
    //     })
    // })
}

function recoverReplicaNodes(){
    con1.query("LOCK TABLE new_recovery_log READ", function (err1) {
        //read con1 logs
        con1.query("SELECT * from new_recovery_log;", function (err, results) {
            con1.query("UNLOCK TABLES", function (err1) {
                if (err1) throw err1;
            });

            results.forEach(resultitem => {
                var txnId = resultitem.transaction_id;
                var type = resultitem.type;
                var name = resultitem.name;
                var year = resultitem.year;
                var rank = resultitem.rank;
                var genre = resultitem.genre;
                var director = resultitem.director;

                var old_name = resultitem.old_name;
                var old_year = resultitem.old_year;
                var old_genre = resultitem.old_genre;
                var old_director = resultitem.old_director;

                con1.query("LOCK TABLE new_recovery_log WRITE, movies_all WRITE", function(err1){
                    con2.query("LOCK TABLE new_recovery_log WRITE, movies_pre1980 WRITE", function(err2){
                        con3.query("LOCK TABLE new_recovery_log WRITE, movies_post1980 WRITE", function(err3){
                            if(resultitem.year < 1980){
                                con2.query("SELECT * from new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err, results){
                                    
                                    //search for corresponding log entry in other node, if not found perform query
                                    if(results.length == 0){ 
                                        if(type == "INSERT"){
                                            con2.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                                con2.commit(function (err) {
                                                    
                                                    //remove from log
                                                    con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                        con1.commit(function (err3) {
                                                            con1.query("UNLOCK TABLES", function(err1){
                                                                console.log("node 1 unlocked");
                                                            });
                                                            con2.query("UNLOCK TABLES", function(err2){
                                                                console.log("node 2 unlocked");
                                                            });
                                                            con3.query("UNLOCK TABLES", function(err2){
                                                                console.log("node 3 unlocked");
                                                            });
                                                        })
                                                    }); 
                                                })
                                            });
                                        }
                                        else if(type == "UPDATE"){
                                            con2.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                                con2.commit(function (err2) {

                                                    //remove from log
                                                    con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                        con1.commit(function (err3) {
                                                            con1.query("UNLOCK TABLES", function(err1){
                                                                console.log("node 1 unlocked");
                                                            });
                                                            con2.query("UNLOCK TABLES", function(err3){
                                                                console.log("node 2 unlocked");
                                                            });
                                                            con3.query("UNLOCK TABLES", function(err2){
                                                                console.log("node 3 unlocked");
                                                            });
                                                        });
                                                    });
                                                });
                                            });
                                        }
                                    }
                                    else{
                                        con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(er1r){
                                            con1.commit(function (err1) {
                                                con1.query("UNLOCK TABLES", function(err1){
                                                    console.log("node 1 unlocked");
                                                });
                                                con2.query("UNLOCK TABLES", function(err3){
                                                    console.log("node 2 unlocked");
                                                });
                                                con3.query("UNLOCK TABLES", function(err2){
                                                    console.log("node 3 unlocked");
                                                });
                                            })
                                        });

                                        con2.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err2){
                                            con1.commit(function (err2) {
                                                con1.query("UNLOCK TABLES", function(err1){
                                                    console.log("node 1 unlocked");
                                                });
                                                con2.query("UNLOCK TABLES", function(err3){
                                                    console.log("node 2 unlocked");
                                                });
                                                con3.query("UNLOCK TABLES", function(err2){
                                                    console.log("node 3 unlocked");
                                                });
                                            })
                                        });
                                    }
                                });
                            }
                            else if(resultitem.year >= 1980){
                                con3.query("SELECT * from new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err, results){
                                    
                                    //search for corresponding log entry in other node, if not found perform query
                                    if(results.length == 0){ 
                                        if(type == "INSERT"){
                                            con3.query("INSERT INTO final_movies_post1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                                con3.commit(function (err) {
                                                    
                                                    //remove from log
                                                    con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                        con1.commit(function (err3) {
                                                            con1.query("UNLOCK TABLES", function(err1){
                                                                console.log("node 1 unlocked");
                                                            })
                                                            con2.query("UNLOCK TABLES", function(err3){
                                                                console.log("node 2 unlocked");
                                                            });
                                                            con3.query("UNLOCK TABLES", function(err2){
                                                                console.log("node 3 unlocked");
                                                            })
                                                        })
                                                    }); 
                                                })
                                            });
                                        }
                                        else if(type == "UPDATE"){
                                            con3.query("UPDATE final_movies_post1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                                con3.commit(function (err2) {

                                                    //remove from log
                                                    con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                        con1.commit(function (err3) {
                                                            con1.query("UNLOCK TABLES", function(err1){
                                                                console.log("node 1 unlocked");
                                                            });
                                                            con2.query("UNLOCK TABLES", function(err3){
                                                                console.log("node 2 unlocked");
                                                            });
                                                            con3.query("UNLOCK TABLES", function(err3){
                                                                console.log("node 3 unlocked");
                                                            });
                                                        });
                                                    });
                                                });
                                            });
                                        }
                                    }
                                    else{
                                        con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(er1r){
                                            con1.commit(function (err1) {
                                                con1.query("UNLOCK TABLES", function(err1){
                                                    console.log("node 1 unlocked");
                                                });
                                                con2.query("UNLOCK TABLES", function(err3){
                                                    console.log("node 2 unlocked");
                                                });
                                                con3.query("UNLOCK TABLES", function(err3){
                                                    console.log("node 3 unlocked");
                                                });
                                            })
                                        });

                                        con3.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err2){
                                            con1.commit(function (err2) {
                                                con1.query("UNLOCK TABLES", function(err1){
                                                    console.log("node 1 unlocked");
                                                });
                                                con2.query("UNLOCK TABLES", function(err3){
                                                    console.log("node 2 unlocked");
                                                });
                                                con3.query("UNLOCK TABLES", function(err3){
                                                    console.log("node 3 unlocked");
                                                });
                                            })
                                        });
                                    }
                                });
                            }
                        });
                    });
                });
            });
        })
    })
}

function recoverPrimaryNodeFromNode2(){
    con2.query("LOCK TABLE new_recovery_log READ", function (err2) {
        con2.query("SELECT * from new_recovery_log;", function (err, results) {
            con2.query("UNLOCK TABLES", function (err2) {
                if (err2) throw err2;
            });

            results.forEach(resultitem => {
                var txnId = resultitem.transaction_id;
                var type = resultitem.type;
                var name = resultitem.name;
                var year = resultitem.year;
                var rank = resultitem.rank;
                var genre = resultitem.genre;
                var director = resultitem.director;

                var old_name = resultitem.old_name;
                var old_year = resultitem.old_year;
                var old_genre = resultitem.old_genre;
                var old_director = resultitem.old_director;

                con1.query("LOCK TABLE new_recovery_log WRITE, movies_all WRITE", function(err1){
                    con2.query("LOCK TABLE new_recovery_log WRITE, movies_pre1980 WRITE", function(err2){
                        con1.query("SELECT * from new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err, results){
                                
                            //search for corresponding log entry in other node, if not found perform query
                            if(results.length == 0){ 
                                if(type == "INSERT"){
                                    con1.query("INSERT INTO final_movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                        con1.commit(function (err) {
                                            
                                            //remove from log
                                            con2.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                con2.commit(function (err3) {
                                                    con1.query("UNLOCK TABLES", function(err1){
                                                        console.log("node 1 unlocked");
                                                    });
                                                    con2.query("UNLOCK TABLES", function(err2){
                                                        console.log("node 2 unlocked");
                                                    });
                                                })
                                            }); 
                                        })
                                    });
                                }
                                else if(type == "UPDATE"){
                                    con1.query("UPDATE final_movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                        con1.commit(function (err1) {

                                            //remove from log
                                            con2.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                con1.commit(function (err3) {
                                                    con1.query("UNLOCK TABLES", function(err1){
                                                        console.log("node 1 unlocked");
                                                    });
                                                    con2.query("UNLOCK TABLES", function(err3){
                                                        console.log("node 2 unlocked");
                                                    });
                                                });
                                            });
                                        });
                                    });
                                }
                            }
                            else{
                                con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(er1r){
                                    con1.commit(function (err1) {
                                        con1.query("UNLOCK TABLES", function(err1){
                                            console.log("node 1 unlocked");
                                        });
                                        con2.query("UNLOCK TABLES", function(err3){
                                            console.log("node 2 unlocked");
                                        });
                                    })
                                });

                                con2.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err2){
                                    con1.commit(function (err2) {
                                        con1.query("UNLOCK TABLES", function(err1){
                                            console.log("node 1 unlocked");
                                        });
                                        con2.query("UNLOCK TABLES", function(err3){
                                            console.log("node 2 unlocked");
                                        });
                                    })
                                });
                            }
                        });
                    });
                });
            });
        });
    });
}

function recoverPrimaryNodeFromNode3(){
    con3.query("LOCK TABLE new_recovery_log READ", function (err2) {
        con3.query("SELECT * from new_recovery_log;", function (err, results) {
            con3.query("UNLOCK TABLES", function (err2) {
                if (err2) throw err2;
            });

            results.forEach(resultitem => {
                var txnId = resultitem.transaction_id;
                var type = resultitem.type;
                var name = resultitem.name;
                var year = resultitem.year;
                var rank = resultitem.rank;
                var genre = resultitem.genre;
                var director = resultitem.director;

                var old_name = resultitem.old_name;
                var old_year = resultitem.old_year;
                var old_genre = resultitem.old_genre;
                var old_director = resultitem.old_director;

                con1.query("LOCK TABLE new_recovery_log WRITE, movies_all WRITE", function(err1){
                    con3.query("LOCK TABLE new_recovery_log WRITE, movies_pre1980 WRITE", function(err2){
                        con1.query("SELECT * from new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err, results){
                                
                            //search for corresponding log entry in other node, if not found perform query
                            if(results.length == 0){ 
                                if(type == "INSERT"){
                                    con1.query("INSERT INTO final_movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                        con1.commit(function (err) {
                                            
                                            //remove from log
                                            con3.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                con3.commit(function (err3) {
                                                    con1.query("UNLOCK TABLES", function(err1){
                                                        console.log("node 1 unlocked");
                                                    });
                                                    con3.query("UNLOCK TABLES", function(err2){
                                                        console.log("node 3 unlocked");
                                                    });
                                                })
                                            }); 
                                        })
                                    });
                                }
                                else if(type == "UPDATE"){
                                    con1.query("UPDATE final_movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                        con1.commit(function (err1) {

                                            //remove from log
                                            con3.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                con1.commit(function (err3) {
                                                    con1.query("UNLOCK TABLES", function(err1){
                                                        console.log("node 1 unlocked");
                                                    });
                                                    con3.query("UNLOCK TABLES", function(err3){
                                                        console.log("node 3 unlocked");
                                                    });
                                                });
                                            });
                                        });
                                    });
                                }
                            }
                            else{
                                con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(er1r){
                                    con1.commit(function (err1) {
                                        con1.query("UNLOCK TABLES", function(err1){
                                            console.log("node 1 unlocked");
                                        });
                                        con3.query("UNLOCK TABLES", function(err3){
                                            console.log("node 3 unlocked");
                                        });
                                    })
                                });

                                con3.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err2){
                                    con1.commit(function (err2) {
                                        con1.query("UNLOCK TABLES", function(err1){
                                            console.log("node 1 unlocked");
                                        });
                                        con3.query("UNLOCK TABLES", function(err3){
                                            console.log("node 3 unlocked");
                                        });
                                    })
                                });
                            }
                        });
                    });
                });
            });
        });
    });
}

function recoverAll(){

    con1.query("SET autocommit=0", function(err){
        con2.query("SET autocommit=0", function(err){
            con3.query("SET autocommit=0", function(err){

                con1.query("LOCK TABLE new_recovery_log READ", function(err1){
                    con2.query("LOCK TABLE new_recovery_log READ", function(err2){
                        con3.query("LOCK TABLE new_recovery_low READ", function(err3){
                            //read con1 logs
                            con1.query("SELECT * from new_recovery_log;", function (err, results) {
                                con1.query("UNLOCK TABLES", function (err1) {
                                    if (err1) throw err1;
                                });
            
                                results.forEach(resultitem => {
                                    var txnId = resultitem.transaction_id;
                                    var type = resultitem.type;
                                    var name = resultitem.name;
                                    var year = resultitem.year;
                                    var rank = resultitem.rank;
                                    var genre = resultitem.genre;
                                    var director = resultitem.director;
            
                                    var old_name = resultitem.old_name;
                                    var old_year = resultitem.old_year;
                                    var old_genre = resultitem.old_genre;
                                    var old_director = resultitem.old_director;
            
                                    con1.query("LOCK TABLE new_recovery_log WRITE, movies_all WRITE", function(err1){
                                        con2.query("LOCK TABLE new_recovery_log WRITE, movies_pre1980 WRITE", function(err2){
                                            con3.query("LOCK TABLE new_recovery_log WRITE, movies_post1980 WRITE", function(err3){
                                                if(resultitem.year < 1980){
                                                    con2.query("SELECT * from new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err, results){
                                                        
                                                        //search for corresponding log entry in other node, if not found perform query
                                                        if(results.length == 0){ 
                                                            if(type == "INSERT"){
                                                                con2.query("INSERT INTO final_movies_pre1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                                                    con2.commit(function (err) {
                                                                        
                                                                        //remove from log
                                                                        con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                                            con1.commit(function (err3) {
                                                                                con1.query("UNLOCK TABLES", function(err1){
                                                                                    //console.log("node 1 unlocked");
                                                                                });
                                                                                con2.query("UNLOCK TABLES", function(err2){
                                                                                    //console.log("node 2 unlocked");
                                                                                });
                                                                                con3.query("UNLOCK TABLES", function(err2){
                                                                                    //console.log("node 3 unlocked");
                                                                                });
                                                                            })
                                                                        }); 
                                                                    })
                                                                });
                                                            }
                                                            else if(type == "UPDATE"){
                                                                con2.query("UPDATE final_movies_pre1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                                                    con2.commit(function (err2) {
            
                                                                        //remove from log
                                                                        con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                                            con1.commit(function (err3) {
                                                                                con1.query("UNLOCK TABLES", function(err1){
                                                                                    //console.log("node 1 unlocked");
                                                                                });
                                                                                con2.query("UNLOCK TABLES", function(err3){
                                                                                    //console.log("node 2 unlocked");
                                                                                });
                                                                                con3.query("UNLOCK TABLES", function(err2){
                                                                                    //console.log("node 3 unlocked");
                                                                                });
                                                                            });
                                                                        });
                                                                    });
                                                                });
                                                            }
                                                        }
                                                        else{
                                                            con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(er1r){
                                                                con1.commit(function (err1) {
                                                                    con1.query("UNLOCK TABLES", function(err1){
                                                                        //console.log("node 1 unlocked");
                                                                    });
                                                                    con2.query("UNLOCK TABLES", function(err3){
                                                                        //console.log("node 2 unlocked");
                                                                    });
                                                                    con3.query("UNLOCK TABLES", function(err2){
                                                                        //console.log("node 3 unlocked");
                                                                    });
                                                                })
                                                            });
            
                                                            con2.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err2){
                                                                con1.commit(function (err2) {
                                                                    con1.query("UNLOCK TABLES", function(err1){
                                                                        //console.log("node 1 unlocked");
                                                                    });
                                                                    con2.query("UNLOCK TABLES", function(err3){
                                                                        //console.log("node 2 unlocked");
                                                                    });
                                                                    con3.query("UNLOCK TABLES", function(err2){
                                                                        //console.log("node 3 unlocked");
                                                                    });
                                                                })
                                                            });
                                                        }
                                                    });
                                                }
                                                else if(resultitem.year >= 1980){
                                                    con3.query("SELECT * from new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err, results){
                                                        
                                                        //search for corresponding log entry in other node, if not found perform query
                                                        if(results.length == 0){ 
                                                            if(type == "INSERT"){
                                                                con3.query("INSERT INTO final_movies_post1980 (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                                                    con3.commit(function (err) {
                                                                        
                                                                        //remove from log
                                                                        con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                                            con1.commit(function (err3) {
                                                                                con1.query("UNLOCK TABLES", function(err1){
                                                                                    //console.log("node 1 unlocked");
                                                                                })
                                                                                con2.query("UNLOCK TABLES", function(err3){
                                                                                    //console.log("node 2 unlocked");
                                                                                });
                                                                                con3.query("UNLOCK TABLES", function(err2){
                                                                                    //console.log("node 3 unlocked");
                                                                                })
                                                                            })
                                                                        }); 
                                                                    })
                                                                });
                                                            }
                                                            else if(type == "UPDATE"){
                                                                con3.query("UPDATE final_movies_post1980 SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                                                    con3.commit(function (err2) {
            
                                                                        //remove from log
                                                                        con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                                            con1.commit(function (err3) {
                                                                                con1.query("UNLOCK TABLES", function(err1){
                                                                                    //console.log("node 1 unlocked");
                                                                                });
                                                                                con2.query("UNLOCK TABLES", function(err3){
                                                                                    //console.log("node 2 unlocked");
                                                                                });
                                                                                con3.query("UNLOCK TABLES", function(err3){
                                                                                    //console.log("node 3 unlocked");
                                                                                });
                                                                            });
                                                                        });
                                                                    });
                                                                });
                                                            }
                                                        }
                                                        else{
                                                            con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(er1r){
                                                                con1.commit(function (err1) {
                                                                    con1.query("UNLOCK TABLES", function(err1){
                                                                        //console.log("node 1 unlocked");
                                                                    });
                                                                    con2.query("UNLOCK TABLES", function(err3){
                                                                        //console.log("node 2 unlocked");
                                                                    });
                                                                    con3.query("UNLOCK TABLES", function(err3){
                                                                        //console.log("node 3 unlocked");
                                                                    });
                                                                })
                                                            });
            
                                                            con3.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err2){
                                                                con1.commit(function (err2) {
                                                                    con1.query("UNLOCK TABLES", function(err1){
                                                                        //console.log("node 1 unlocked");
                                                                    });
                                                                    con2.query("UNLOCK TABLES", function(err3){
                                                                        //console.log("node 2 unlocked");
                                                                    });
                                                                    con3.query("UNLOCK TABLES", function(err3){
                                                                        //console.log("node 3 unlocked");
                                                                    });
                                                                })
                                                            });
                                                        }
                                                    });
                                                }
                                            });
                                        });
                                    });
                                });
                            })
            
                            con2.query("SELECT * from new_recovery_log;", function (err, results) {
                                con2.query("UNLOCK TABLES", function (err2) {
                                    if (err2) throw err2;
                                });
                    
                                results.forEach(resultitem => {
                                    var txnId = resultitem.transaction_id;
                                    var type = resultitem.type;
                                    var name = resultitem.name;
                                    var year = resultitem.year;
                                    var rank = resultitem.rank;
                                    var genre = resultitem.genre;
                                    var director = resultitem.director;
                    
                                    var old_name = resultitem.old_name;
                                    var old_year = resultitem.old_year;
                                    var old_genre = resultitem.old_genre;
                                    var old_director = resultitem.old_director;
                    
                                    con1.query("LOCK TABLE new_recovery_log WRITE, movies_all WRITE", function(err1){
                                        con2.query("LOCK TABLE new_recovery_log WRITE, movies_pre1980 WRITE", function(err2){
                                            con1.query("SELECT * from new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err, results){
                                                    
                                                //search for corresponding log entry in other node, if not found perform query
                                                if(results.length == 0){ 
                                                    if(type == "INSERT"){
                                                        con1.query("INSERT INTO final_movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                                            con1.commit(function (err) {
                                                                
                                                                //remove from log
                                                                con2.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                                    con2.commit(function (err3) {
                                                                        con1.query("UNLOCK TABLES", function(err1){
                                                                            //console.log("node 1 unlocked");
                                                                        });
                                                                        con2.query("UNLOCK TABLES", function(err2){
                                                                            //console.log("node 2 unlocked");
                                                                        });
                                                                    })
                                                                }); 
                                                            })
                                                        });
                                                    }
                                                    else if(type == "UPDATE"){
                                                        con1.query("UPDATE final_movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                                            con1.commit(function (err1) {
                    
                                                                //remove from log
                                                                con2.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                                    con1.commit(function (err3) {
                                                                        con1.query("UNLOCK TABLES", function(err1){
                                                                            //console.log("node 1 unlocked");
                                                                        });
                                                                        con2.query("UNLOCK TABLES", function(err3){
                                                                            //console.log("node 2 unlocked");
                                                                        });
                                                                    });
                                                                });
                                                            });
                                                        });
                                                    }
                                                }
                                                else{
                                                    con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(er1r){
                                                        con1.commit(function (err1) {
                                                            con1.query("UNLOCK TABLES", function(err1){
                                                                //console.log("node 1 unlocked");
                                                            });
                                                            con2.query("UNLOCK TABLES", function(err3){
                                                                //console.log("node 2 unlocked");
                                                            });
                                                        })
                                                    });
                    
                                                    con2.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err2){
                                                        con1.commit(function (err2) {
                                                            con1.query("UNLOCK TABLES", function(err1){
                                                                //console.log("node 1 unlocked");
                                                            });
                                                            con2.query("UNLOCK TABLES", function(err3){
                                                                //console.log("node 2 unlocked");
                                                            });
                                                        })
                                                    });
                                                }
                                            });
                                        });
                                    });
                                });
                            });
            
                            con3.query("SELECT * from new_recovery_log;", function (err, results) {
                                con3.query("UNLOCK TABLES", function (err2) {
                                    if (err2) throw err2;
                                });
                    
                                results.forEach(resultitem => {
                                    var txnId = resultitem.transaction_id;
                                    var type = resultitem.type;
                                    var name = resultitem.name;
                                    var year = resultitem.year;
                                    var rank = resultitem.rank;
                                    var genre = resultitem.genre;
                                    var director = resultitem.director;
                    
                                    var old_name = resultitem.old_name;
                                    var old_year = resultitem.old_year;
                                    var old_genre = resultitem.old_genre;
                                    var old_director = resultitem.old_director;
                    
                                    con1.query("LOCK TABLE new_recovery_log WRITE, movies_all WRITE", function(err1){
                                        con3.query("LOCK TABLE new_recovery_log WRITE, movies_pre1980 WRITE", function(err2){
                                            con1.query("SELECT * from new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err, results){
                                                    
                                                //search for corresponding log entry in other node, if not found perform query
                                                if(results.length == 0){ 
                                                    if(type == "INSERT"){
                                                        con1.query("INSERT INTO final_movies_all (name, year, `rank`, genre, director) VALUES (?,?,?,?,?);", [name, year, rank, genre, director], function (err) {
                                                            con1.commit(function (err) {
                                                                
                                                                //remove from log
                                                                con3.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                                    con3.commit(function (err3) {
                                                                        con1.query("UNLOCK TABLES", function(err1){
                                                                            //console.log("node 1 unlocked");
                                                                        });
                                                                        con3.query("UNLOCK TABLES", function(err2){
                                                                            //console.log("node 3 unlocked");
                                                                        });
                                                                    })
                                                                }); 
                                                            })
                                                        });
                                                    }
                                                    else if(type == "UPDATE"){
                                                        con1.query("UPDATE final_movies_all SET name=?, year=?, `rank`=?, genre=?, director=? WHERE name=? AND year=? AND genre=? AND director=?;", [name, year, rank, genre, director, old_name, old_year, old_genre, old_director], function (err) {
                                                            con1.commit(function (err1) {
                    
                                                                //remove from log
                                                                con3.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(err){
                                                                    con1.commit(function (err3) {
                                                                        con1.query("UNLOCK TABLES", function(err1){
                                                                            //console.log("node 1 unlocked");
                                                                        });
                                                                        con3.query("UNLOCK TABLES", function(err3){
                                                                            //console.log("node 3 unlocked");
                                                                        });
                                                                    });
                                                                });
                                                            });
                                                        });
                                                    }
                                                }
                                                else{
                                                    con1.query("DELETE FROM new_recovery_log WHERE transaction_id=?;", [txnId], function(er1r){
                                                        con1.commit(function (err1) {
                                                            con1.query("UNLOCK TABLES", function(err1){
                                                                //console.log("node 1 unlocked");
                                                            });
                                                            con3.query("UNLOCK TABLES", function(err3){
                                                                //console.log("node 3 unlocked");
                                                            });
                                                        })
                                                    });
                    
                                                    con3.query("DELETE FROM new_recovery_log WHERE type=? AND name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [type, name, year, rank, genre, director], function(err2){
                                                        con1.commit(function (err2) {
                                                            con1.query("UNLOCK TABLES", function(err1){
                                                                //console.log("node 1 unlocked");
                                                            });
                                                            con3.query("UNLOCK TABLES", function(err3){
                                                                //console.log("node 3 unlocked");
                                                            });
                                                        })
                                                    });
                                                }
                                            });
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            })
        })
    })
}

function closeAllConnection() {
    con1.end(function (err) {
        if (err) throw err;
        console.log("Closed connection " + 1);
    });
    con2.end(function (err) {
        if (err) throw err;
        console.log("Closed connection " + 2);
    });
    con3.end(function (err) {
        if (err) throw err;
        console.log("Closed connection " + 3);
    });
}
module.exports = { newSearch, reallyNewUpdate, reallyNewInsert, closeAllConnection, generateAllReports, InsertSimulatePrimaryError, InsertSimulateReplicaError, recoverAll};
// module.exports = { closeConnection, searchRecord, insertOneRecordIntoAllNodes };
