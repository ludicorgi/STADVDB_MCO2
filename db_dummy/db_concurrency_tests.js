const db = require('./db_functions_test');
const connections = require('./db_connections');
const { closeConnection } = require('./db_functions');
const con1 = connections.con1;
const con2 = connections.con2;
const con3 = connections.con3;
const con1Clone = connections.con1Clone;
const con2Clone = connections.con2Clone;
const con3Clone = connections.con3Clone;
const con3Clone2 = connections.con3Clone2;
async function concurrencyTest1() {
    // all transactions are reading
    let t1res, t2res, fixedRes;
    console.log("Test 1");
    con1.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE id = 1;", (err, res) => {
        if (err) throw err
        fixedRes = JSON.stringify(res);
    })
    con1.beginTransaction((err) => {
        if (err) throw err
        con1.query("LOCK TABLES movies_all READ", (err) => {
            if (err) throw err
            con1.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE id = 1;", (err, res) => {
                if (err) throw err
                console.log("Transaction 1:");
                // console.log(res);
                t1res = JSON.stringify(res);
                con1.query("DO SLEEP(2)", (err) => {
                    if (err) throw err
                    con1.commit((err) => {
                        if (err) throw err
                        console.log("T1 Done");
                        con1.query("UNLOCK TABLES", (err) => {
                            if (err) throw err
                        })
                    })
                })
            })
        })
    })

    await sleep(3000);
    con1Clone.beginTransaction((err) => {
        if (err) throw err
        con1Clone.query("LOCK TABLES movies_all READ", (err) => {
            if (err) throw err
            con1Clone.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE id = 1;", (err, res) => {
                if (err) throw err
                console.log("Transaction 2:")
                // console.log(res[0].rank);
                t2res = JSON.stringify(res);
                con1Clone.commit((err) => {
                    if (err) throw err
                    console.log("T2 Done");
                    con1Clone.query("UNLOCK TABLES", (err) => {
                        if (err) throw err
                    })
                })
            })
        })
    })


    await sleep(1000);
    if (t1res == fixedRes && t2res == fixedRes) {
        console.log("Pass");
    } else console.log("Fail");
    // console.log(t1res);

}

async function concurrencyTest2(option) {
    let t2res, t3res;
    let rank // 5 or null
    console.log("Test 2");
    if (option % 2 == 0) {
        rank = 5
    } else {
        rank = 7
    }
    con1.beginTransaction((err) => {
        if (err) throw err
        con1.query("LOCK TABLE movies_all WRITE", (err) => {
            if (err) throw err
            con3.query("LOCK TABLE movies_post1980 WRITE", (err) => {
                if (err) throw err
                console.log("Transaction 1:")
                con1.query("UPDATE movies_all SET `rank`= " + rank + " WHERE id=1", (err, res) => { // rank is null before
                    console.log("Transaction 1 Node 1 updated");
                    if (err) throw err
                    // con1.query("DO SLEEP(20)", (err) => {
                    con3.beginTransaction((err) => {
                        console.log("Sub transaction 1 started");
                        if (err) throw err
                        con3.query("UPDATE movies_post1980 SET `rank`= " + rank + " WHERE id=1", (err) => {
                            console.log("Transaction 1 Node 3 updated");
                            if (err) throw err
                            con3.query("DO SLEEP(4)", (err) => {
                                if (err) throw err
                                con3.commit((err) => {
                                    if (err) throw err
                                    console.log("Transaction 1 Node 3 committed");
                                    con1.query("UNLOCK TABLES", (err) => {
                                        if (err) throw err
                                        con3.query("UNLOCK TABLES", (err) => {
                                            if (err) throw err
                                            con1.commit((err) => {
                                                console.log("Transaction 1 End Transaction");
                                                if (err) throw err
                                            })
                                        })
                                    })
                                });
                            });
                        });
                    });
                    // });
                });
            })
        });
    })
    con1.query('SELECT @@transaction_ISOLATION;', (err, res) => {
        console.log(res);
    })
    await sleep(1000);
    con1Clone.beginTransaction((err) => {
        if (err) throw err
        console.log("Transaction 2:");
        con1Clone.query("LOCK TABLE movies_all READ", (err) => {
            if (err) throw err
            con1Clone.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE id = 1;", (err, res) => {
                if (err) throw err
                con1Clone.query("UNLOCK TABLES", (err) => {
                    if (err) throw err
                    con1Clone.commit((err) => {
                        t2res = res[0].rank;
                        console.log("Transaction 2 committed", [t2res, rank]);
                        if (err) throw err
                    })
                })
            })
        })
    })
    await sleep(1000);
    con3Clone.beginTransaction((err) => {
        if (err) throw err
        console.log("Transaction 3:");
        con3Clone.query("LOCK TABLE movies_post1980 READ", (err) => {
            con3Clone.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_post1980 WHERE id = 1;", (err, res) => {
                if (err) throw err
                con3Clone.query("UNLOCK TABLES", (err) => {
                    if (err) throw err
                    con3Clone.commit((err) => {
                        t3res = res[0].rank;
                        console.log("Transaction 3 committed", [t3res, rank]);
                        if (err) throw err
                    })
                })
            })
        })
    })
    await sleep(8000);
    // T2 should be updated
    if (t2res == rank) {
        console.log("T2 " + t2res + ":" + rank);
        console.log('T2 pass');
    } else {
        console.log("T2 " + t2res + ":" + rank);
        console.log('T2 fail');
    }

    if (t3res == rank) {
        console.log("T3 " + t3res + ":" + rank);
        console.log('T3 pass');
    } else {
        console.log("T3 " + t3res + ":" + rank);
        console.log('T3 fail');
    }

    await sleep(1000);
    con3.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_post1980 WHERE id = 1;", (err, res) => {
        if (err) throw err

        if (res[0].rank == rank) console.log("node3 updated");
    })


}

async function concurrencyTest3(option) {
    let rank; // null or 5
    console.log("Test 3");
    if (option % 2 == 0) {
        rank = 5
    } else {
        rank = 7
    }
    con3Clone.query('SELECT @@transaction_ISOLATION;', (err, res) => {
        console.log(res);
    })
    // let year = 2000  // id 1; year 2000 is original
    con1.beginTransaction((err) => {
        if (err) throw err
        console.log("T1 Started");
        con1.query("LOCK TABLE movies_all WRITE", (err) => {
            con3.query("LOCK TABLE movies_post1980 WRITE", (err) => {
                con1.query("UPDATE movies_all SET `rank`= " + rank + " WHERE id=1", (err, res) => { // rank is null before
                    // console.log(res);
                    if (err) throw err
                    con1.query("DO SLEEP(8)", (err) => {
                        if (err) throw err
                        if (err) throw err
                        con3.beginTransaction((err) => {
                            if (err) throw err
                            console.log("Sub transaction 1 Started");
                            con3.query("UPDATE movies_post1980 SET `rank`= " + rank + " WHERE id=1", (err) => {
                                if (err) throw err
                                con3.query("DO SLEEP(4)", (err) => {
                                    if (err) throw err
                                    con3.commit((err) => {
                                        console.log("Sub transaction 1 Committed");
                                        if (err) throw err
                                        con1.query("UNLOCK TABLES", (err) => {
                                            if (err) throw err
                                            con3.query("UNLOCK TABLES", (err) => {
                                                if (err) throw err
                                                con1.commit((err) => {
                                                    if (err) throw err
                                                })
                                            })
                                        })
                                    });
                                });
                            });
                        });
                    });
                });
            })
        })

    });
    await sleep(4000);
    con1Clone.query("LOCK TABLE movies_all WRITE", (err) => {
        con3Clone.query("LOCK TABLE movies_post1980 WRITE", (err) => {
            con1Clone.beginTransaction((err) => {
                if (err) throw err
                console.log("T2 Started");
                con1Clone.query("UPDATE movies_all SET `rank`= `rank` + 1 WHERE id=1", (err, res) => { // rank is null before
                    // console.log(res);
                    if (err) throw err
                    con3Clone.beginTransaction((err) => {
                        console.log("Transaction 2 Subtransaction Started");
                        if (err) throw err
                        con3Clone.query("UPDATE movies_post1980 SET `rank`= `rank` + 1 WHERE id=1", (err) => {
                            if (err) throw err
                            con3Clone.commit((err) => {
                                console.log("T2S Committed");
                                if (err) throw err
                                con3Clone.query("UNLOCK TABLES", (err) => {
                                    if (err) throw err
                                    con1Clone.query("UNLOCK TABLES", (err) => {
                                        if (err) throw err
                                        con1Clone.commit((err) => {
                                            if (err) throw err
                                        })
                                    })
                                })
                            });
                        });
                    });
                });
            });
        })
    })

    // con3Clone2.query("LOCK TABLE movies_post1980 WRITE", (err) => {
    //     if(err) throw err
    //     con3Clone2.beginTransaction((err) => {
    //         if (err) throw err
    //         console.log("T3 Started");
    //         if (err) throw err
    //         con3Clone2.query("UPDATE movies_post1980 SET `rank`= `rank` + 1 WHERE id=1", (err) => {
    //             if (err) throw err
    //             con3Clone2.commit((err) => {
    //                 console.log("T3 Committed");
    //                 if (err) throw err
    //                 con3Clone2.query("UNLOCK TABLES", (err) => {
    //                     if(err) throw err
    //                 })
    //             });
    //         });
    //     })
    // });

    await sleep(20000);
    con1.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE id = 1;", (err, res) => {
        if (res[0].rank == rank + 1) {
            console.log("Node 1 pass", res[0].rank, rank + 1);
        } else {
            console.log("Node 1 fail", res[0].rank, rank + 1);
        }
    })

    con3.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_post1980 WHERE id = 1;", (err, res) => {
        if (res[0].rank == rank + 1) {
            console.log("Node 3 pass", res[0].rank, rank + 1);
        } else {
            console.log("Node 3 fail", res[0].rank, rank + 1);
        }
    })

}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}
async function runAllTests() {

    for (let i = 0; i < 4; i++) {
        if (i == 0) db.setAllIsolationLevel('read uncommitted');
        else if (i == 1) db.setAllIsolationLevel('read committed');
        else if (i == 2) db.setAllIsolationLevel('repeatable read');
        else if (i == 3) db.setAllIsolationLevel('serializable');
        await sleep(2000);
        await concurrencyTest1();
        console.log("----");
        await concurrencyTest2(i);
        console.log("----");
        await concurrencyTest3(i);
    }

}

runAllTests();
