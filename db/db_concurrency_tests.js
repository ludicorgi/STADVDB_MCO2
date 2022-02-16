const db = require('./db_functions');
const connections = require('./db_connections');
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
    let t1res2, t2res2;
    console.log("Test 1");
    let str = "Test 1\n";
    //  boire rank is null
    con1.query("SELECT `name`, `year`, `rank`, genre, director FROM final_movies_all WHERE name = ' boire';", (err, res) => {
        if (err) throw err
        fixedRes = JSON.stringify(res);
    })
    con1.beginTransaction((err) => {
        console.log("Transaction 1:");
        str += "Transaction 1:\n"
        if (err) throw err
        con1.query("LOCK TABLES final_movies_all READ", (err) => {
            if (err) throw err
            con3.query("LOCK TABLES final_movies_post1980 READ", (err) => {
                if (err) throw err
                con1.query("DO SLEEP(2)", (err) => {
                    if (err) throw err
                    con1.query("SELECT `name`, `year`, `rank`, genre, director FROM final_movies_all WHERE name = ' boire';", (err, res) => {
                        if (err) throw err
                        // console.log(res);
                        t1res = JSON.stringify(res);
                        con1.query("SELECT `name`, `year`, `rank`, genre, director FROM final_movies_all WHERE name = ' boire'", (err, res) => {
                            if (err) throw err
                            t1res2 = JSON.stringify(res)
                            con1.query("UNLOCK TABLES", (err) => {
                                if (err) throw err
                                str += "T1 Done\n"
                                console.log("T1 Done");
                                con1.commit((err) => {
                                    if (err) throw err
                                })
                            })
                        })
                    })
                })
            })
        })
    })

    await sleep(1000);
    con1Clone.beginTransaction((err) => {
        str += ("Transaction 2:\n")
        console.log("Transaction 2:")
        if (err) throw err
        con1Clone.query("LOCK TABLES final_movies_all READ", (err) => {
            if (err) throw err
            con3.query("LOCK TABLES final_movies_post1980 READ", (err) => {
                if (err) throw err
                con1Clone.query("SELECT `name`, `year`, `rank`, genre, director FROM final_movies_all WHERE name = ' boire';", (err, res) => {
                    if (err) throw err
                    // console.log(res);
                    t2res = JSON.stringify(res);
                    con3Clone.query("SELECT `name`, `year`, `rank`, genre, director FROM final_movies_post1980 WHERE name = ' boire';", (err, res) => {
                        if (err) throw err
                        t2res2 = JSON.stringify(res);
                        con1Clone.query("UNLOCK TABLES", (err) => {
                            if (err) throw err
                            str += ("T2 Done\n")
                            console.log("T2 Done");
                            con1Clone.commit((err) => {
                                if (err) throw err
                            })
                        })
                    })
                })
            })
        })
    })


    await sleep(2000);
    if (t1res == fixedRes && t2res == fixedRes && t1res2 == t1res && t2res2 == t1res2) {
        console.log("Pass", [fixedRes, t1res, t2res, t1res2, t2res2]);
        // console.log(str);
        str += t1res + "\n";
        str += t2res2 + "\n";
        return (str);
    } else {
        console.log("Fail", [fixedRes, t1res, t2res, t1res2, t2res2]);
        return str;
    }
    // console.log(t1res);

}

async function concurrencyTest2(option) {
    let t2res, t3res;
    let rank // 5 or 7
    console.log("Test 2");
    let str = "Test 2 \n";
    if (option % 2 == 0) {
        rank = 5
    } else {
        rank = 7
    }
    con3.query("SET autocommit = 0", (err) => {
        if (err) console.error(err);
        console.log("Transaction 2:");
        str += ("Transaction 2\n")
        con3.query("LOCK TABLE final_movies_post1980 WRITE", (err) => {
            console.log("Node 3 Locked T2");
            str += ("Node 3 Locked T2\n")
            if (err) console.error(err);
            con3.query("UPDATE final_movies_post1980 SET `rank`= " + rank + " WHERE name = ' boire'", (err) => {
                if (err) console.error(err);
                con3.query("DO SLEEP(8)", (err) => {
                    if (err) console.error(err);
                    str += ("Node 3 Sleep 8\n")
                    console.log("Node 3 Sleep 8");
                    con3.query("UNLOCK TABLES", (err) => {
                        if (err) console.error(err);
                        str += ("Transaction 2 Node 3 Unlocked\n")
                        console.log("Transaction 2 Node 3 Unlocked");
                        con3.commit((err) => {
                            if (err) console.error(err);
                            console.log("Transaction 2 End Transaction");
                            str += ("Transaction 2 End Transaction\n");
                        })
                    })
                })
            })
        })
    })

    con1.query("SET autocommit = 0", async (err) => {
        console.log("Transaction 1:");
        str += ("Transaction 1:\n");
        con1.query("LOCK TABLE final_movies_all WRITE", (err) => {
            console.log("Node 1 Locked T1");
            str += ("Node 1 Locked T1\n");
            con1.query("UPDATE final_movies_all SET `rank`= " + rank + " WHERE name = ' boire'", (err, res) => { // rank is null before
                con1.query("DO SLEEP(8)", (err) => {
                    console.log("Node 1 Sleep 8");
                    str += ("Node 1 Sleep 8\n")
                    con1.query("UNLOCK TABLES", (err) => {
                        console.log("Transaction 1 Node 1 UNLOCKED");
                        str += ("Transaction 1 Node 1 UNLOCKED\n");
                        con1.commit((err) => {
                            console.log("Transaction 1 End Transaction");
                            str += ("Transaction 1 End Transaction\n")
                            if (err) throw err
                        })
                    })
                })
            });
        });
    })
    await sleep(1000);

    // read node 1
    con1Clone.query("SET autocommit = 0", (err) => {
        if (err) throw err
        console.log("Transaction 3:");
        str += ("Transaction 3:\n")
        con1Clone.query("LOCK TABLE final_movies_all READ", (err) => {
            console.log("Transaction 3 Node 1 Locked");
            str += ("Transaction 3 Node 1 Locked\n");
            if (err) throw err
            con1Clone.query("SELECT `name`, `year`, `rank`, genre, director FROM final_movies_all WHERE name = ' boire';", (err, res) => {
                if (err) throw err
                con1Clone.commit((err) => {
                    if (err) throw err
                    con1Clone.query("UNLOCK TABLES", (err) => {
                        // console.log(res[0]);
                        str += ("Transaction 3 Node 1 Unlocked\n")
                        t2res = res[0].rank;
                        console.log("Transaction 3 committed", [t2res, rank]);
                        str += ("Transaction 3 committed " + [t2res, rank] + "\n");
                        if (err) throw err
                    })
                })
            })
        })
    })
    // read node 3
    con3Clone.query("SET autocommit = 0", (err) => {
        if (err) throw err
        console.log("Transaction 4:");
        str += ("Transaction 4:\n")
        con3Clone.query("LOCK TABLE final_movies_post1980 READ", (err) => {
            if (err) throw err
            console.log("Transaction 4 Node 3 Locked");
            str += ("Transaction 4 Node 3 Locked\n");
            con3Clone.query("SELECT `name`, `year`, `rank`, genre, director FROM final_movies_post1980 WHERE name = ' boire';", (err, res) => {
                if (err) throw err
                con3Clone.commit((err) => {
                    if (err) throw err
                    con3Clone.query("UNLOCK TABLES", (err) => {
                        // console.log(res);
                        str += ("Transaction 4 Node 3 Unlocked\n")
                        console.log(res);
                        t3res = res[0].rank;
                        console.log("Transaction 4 committed", [t3res, rank]);
                        str += ("Transaction 4 committed " + [t3res, rank] + "\n");
                        if (err) throw err
                    })
                })
            })
        })
    })
    con1.query('SELECT @@transaction_ISOLATION;', (err, res) => {
        console.log(res);
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

    con3.query("SELECT `name`, `year`, `rank`, genre, director FROM final_movies_post1980 WHERE name = ' boire';", (err, res) => {
        if (err) throw err
        if (res[0].rank == rank) console.log("node3 updated");
    })
    console.log("Log ", str.length, str);
    return (str);


}

async function concurrencyTest3(option) {
    let rank; // null or 5
    let str = "Test 3\n";
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
    con3.query("SET autocommit = 0", async (err) => {
        console.log("T2 Started");
        str += ("T2 Started\n")
        con3.query("LOCK TABLE final_movies_post1980 WRITE", (err) => {
            console.log("Node 3 Locked T2");
            str += ("Node 3 Locked T2\n")
            con3.query("UPDATE final_movies_post1980 SET `rank`= " + rank + " WHERE name = ' boire'", (err) => {
                con3.query("DO SLEEP(4)", (err) => {
                    console.log("Node 3 Sleep 4 T2");
                    str += ("Node 3 Sleep 4 T2\n");
                    con3.commit((err) => {
                        console.log(("Transaction 2 Committed"));
                        str += ("Transaction 2 Committed\n");
                        con3.query("UNLOCK TABLES", (err) => {
                            console.log("Node 3 Unlocked T2");
                            str += ("Node 3 Unlocked T2\n")
                        })
                    })
                })
            })
        })
    })
    con1.query("SET autocommit = 0", async (err) => {
        if (err) throw err
        console.log("T1 Started");
        str += ("T1 Started\n");
        con1.query("LOCK TABLE final_movies_all WRITE", (err) => {
            console.log("Node 1 Locked T1");
            str += ("Node 1 Locked T1\n");
            con1.query("UPDATE final_movies_all SET `rank`= " + rank + " WHERE name = ' boire'", (err, res) => { // rank is null before
                con1.query("DO SLEEP(8)", (err) => {
                    console.log("Node 1 Sleep 8 T1");
                    str += ("Node 1 Sleep 8 T1\n")
                    con1.commit((err) => {
                        console.log("Transaction 1 Committed");
                        str += ("Transaction 1 Committed\n");
                        con1.query("UNLOCK TABLES", (err) => {
                            console.log("Node 1 Unlocked T1");
                            str += ("Node 1 Unlocked T1\n");
                        })
                    });
                });
            });
        });
    });
    await sleep(1000);
    con3Clone.query("SET autocommit = 0", (err) => {
        console.log("T4 Started");
        str += ("T4 Started\n");
        con3Clone.query("LOCK TABLE final_movies_post1980 WRITE", (err) => {
            console.log("Node 3 Locked T4");
            str += ("Node 3 Locked T4\n")
            con3Clone.query("UPDATE final_movies_post1980 SET `rank`= `rank` + 1 WHERE name = ' boire'", (err) => {
                con3Clone.commit((err) => {
                    console.log("T4 Committed");
                    str += ("T4 Committed\n");
                    con3Clone.query("UNLOCK TABLES", (err) => {
                        console.log("Node 3 T4 Unlocked");
                        str += ("Node 3 T4 Unlocked\n");
                    })
                })
            })
        })
    })
    con1Clone.query("SET autocommit = 0", (err) => {
        console.log("T3 Started");
        str += ("T3 Started\n")
        con1Clone.query("LOCK TABLE final_movies_all WRITE", (err) => {
            console.log("Node 1 Locked T3");
            str += ("Node 1 Locked T3\n")
            con1Clone.query("UPDATE final_movies_all SET `rank`= `rank` + 1 WHERE name = ' boire'", (err, res) => { // rank is null before
                con1Clone.commit((err) => {
                    console.log("T3 Committed");
                    str += ("T3 Committed\n")
                    con1Clone.query("UNLOCK TABLES", (err) => {
                        console.log("Node 1 T3 Unlocked");
                        str += ("Node 1 T3 Unlocked\n");
                    })
                })
            });
        });
    });
    await sleep(10000)
    let c1, c3;
    con1.query("LOCK TABLES final_movies_all READ", (err) => {
        con1.query("SELECT `name`, `year`, `rank`, genre, director FROM final_movies_all WHERE name = ' boire';", (err, res) => {
            c1 = true;
            if (res[0].rank == rank + 1) {
                console.log("Node 1 pass", res[0].rank, rank + 1);
                str += ("Node 1 pass" + res[0].rank + " " + (rank + 1) + "\n");
            } else {
                console.log("Node 1 fail", res[0].rank, rank + 1);
                str += ("Node 1 fail" + res[0].rank + " " + (rank + 1) + "\n");
            }
            con1.query("UNLOCK TABLES")
        })
    })

    con3.query("LOCK TABLES final_movies_post1980 READ", (err) => {
        con3.query("SELECT `name`, `year`, `rank`, genre, director FROM final_movies_post1980 WHERE name = ' boire'", (err, res) => {
            c3 = true;
            if (res[0].rank == rank + 1) {
                console.log("Node 3 pass", res[0].rank, rank + 1);
                str += ("Node 3 pass" + res[0].rank + " " + (rank + 1) + "\n");
            } else {
                console.log("Node 3 fail", res[0].rank, rank + 1);
                str += ("Node 3 fail" + res[0].rank + " " + (rank + 1) + "\n");
            }
            con3.query("UNLOCK TABLES")
        })
    })
    await sleep(5000);
    if (c1 && c3) {
        return str;
    }
    // con3Clone2.query("LOCK TABLE final_movies_post1980 WRITE", (err) => {
    //     if(err) throw err
    //     con3Clone2.beginTransaction((err) => {
    //         if (err) throw err
    //         console.log("T3 Started");
    //         if (err) throw err
    //         con3Clone2.query("UPDATE final_movies_post1980 SET `rank`= `rank` + 1 WHERE id=1", (err) => {
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



}

async function repeatedRead() {
    let t1res, t2res;

    con1.query("SET autocommit = 0", (err) => {
        console.log("T1:");
        con1.query("LOCK TABLE final_movies_all READ", (err) => {
            con1.query("SELECT * FROM final_movies_all WHERE name = '$'", (err, res) => {
                // console.log(res);
                t1res = res;
                con1.query("DO SLEEP (4)", (err) => {
                    console.log("T2:");
                    con1.query("SELECT * FROM final_movies_all WHERE name = '$'", (err, res) => {
                        // console.log(res);
                        t2res = res;
                        con1.commit((err) => {
                            con1.query("UNLOCK TABLES", (err) => {
                                console.log("");
                            })
                        })
                    })
                })
            })
        })
    })
    await sleep(1000)
    con1Clone.query("SET autocommit = 0", (err) => {
        con1Clone.query("LOCK TABLE final_movies_all WRITE", (err) => {
            con1Clone.query("UPDATE final_movies_all SET `rank` = 6.4 WHERE name ='$'", (err) => {
                con1Clone.commit((err) => {
                    con1Clone.query("UNLOCK TABLES", (err) => {
                        console.log("done");
                    })
                })
            })
        })
    })
    await sleep(5000)
    console.log("RES: ", t1res, t2res);
}

async function dirtyRead() {
    con1.query("Set autocommit = 0", (err) => {
        con1.query("LOCK TABLE final_movies_all WRITE", (err) => {
            con1.query("UPDATE final_movies_all SET `rank` = 2 WHERE name ='$'", (err) => {
                console.log("after update");
                con1.query("DO SLEEP(4)", (err) => {
                    con1.commit((err) => {
                        con1.query("UNLOCK TABLES", (err) => {
                            console.log("done");
                        })
                    })
                })
            })
        })
    })
    await sleep(1000)
    con1Clone.query("SET autocommit = 0", (err) => {
        con1Clone.query("LOCK TABLE final_movies_all READ", (err) => {
            con1Clone.query("SELECT * FROM final_movies_all WHERE name='$'", (err, res) => {
                t2res = res;
                con1Clone.commit((err) => {
                    con1Clone.query("UNLOCK TABLES", (err) => {
                        console.log("read done");
                        console.log(t2res);
                    })
                })
            })
        })
    })
}

// repeatedRead();
// dirtyRead();

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}
async function runAllTests(callback) {
    let t1 = "";
    let t2 = "";
    let t3 = "";
    for (let i = 2; i < 3; i++) {
        if (i == 0) {
            db.setAllIsolationLevel('read uncommitted');
            t1 += "Read Uncommitted \n\n"
            t2 += "Read Uncommitted \n\n"
            t3 += "Read Uncommitted \n\n"
        } else if (i == 1) {
            db.setAllIsolationLevel('read committed');
            t1 += "Read Committed \n\n"
            t2 += "Read Committed \n\n"
            t3 += "Read Committed \n\n"
        } else if (i == 2) {
            db.setAllIsolationLevel('repeatable read');
            t1 += "Repeatable Read \n\n"
            t2 += "Repeatable Read \n\n"
            t3 += "Repeatable Read \n\n"
        } else if (i == 3) {
            db.setAllIsolationLevel('serializable');
            t1 += "Serializable \n\n"
            t2 += "Serializable \n\n"
            t3 += "Serializable \n\n"
        }
        // await sleep(5000);
        t1r = await concurrencyTest1();
        t1 += t1r;
        console.log("----");
        t2r = await concurrencyTest2();
        t2 += t2r;
        console.log("----");
        t3r = await concurrencyTest3();
        t3 += t3r;
    }
    // console.log("T1 Log: ", t1);
    // console.log("T2 Log: ", t2);
    // console.log("T3 Log: ", t3);
    let res = [t1, t2, t3]
    // console.log("res: ",res.length);
    callback(res);
}

async function testSearch() {
    // director >1980 Hye Jung Park 2
    // director <1980 Frank Moser 442
    // year >1980 1979 3559
    // year <1980 1981 3511

    // db.newSearch('year', '1981', (res)=>{
    //     console.log(res[0],res.length);
    // });
}
// testSearch();
// runAllTests((res)=>{
//     console.log("Logs: ");
//     console.log(res[0]);
//     console.log(res[1]);
//     console.log(res[2]);
// });
module.exports = { runAllTests };
