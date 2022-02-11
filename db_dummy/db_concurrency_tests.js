const db = require('./db_functions_test');
const connections = require('./db_connections');
const { closeConnection } = require('./db_functions');
const con1 = connections.con1;
const con2 = connections.con2;
const con3 = connections.con3;
const con1Clone = connections.con1Clone;
async function concurrencyTest1(){
    // all transactions are reading
    let t1res, t2res, fixedRes;
    con1.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE id = 1;", (err, res)=> {
        if(err) throw err
        fixedRes = JSON.stringify(res);
    })

    con1.beginTransaction((err) => {
        if(err) throw err

        con1.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE id = 1;", (err, res)=> {
            if(err) throw err

            console.log("Transaction 1:");
            // console.log(res);
            t1res = JSON.stringify(res);
            con1.query("DO SLEEP(2)", (err) => {
                if(err) throw err
                con1.commit((err) => {
                    if(err) throw err
                    console.log("T1 Done");
                })
            })

        })
        
    })
    await sleep(3000);
    con1Clone.beginTransaction((err) => {
        if(err) throw err
        
        con1Clone.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE id = 1;", (err, res)=> {
            if(err) throw err

            console.log("Transaction 2:")
            // console.log(res[0].rank);
            t2res = JSON.stringify(res);
            con1Clone.commit((err) => {
                if(err) throw err
                console.log("T2 Done");
            })
        })
    })

    await sleep(1000);
    if(t1res == fixedRes && t2res == fixedRes){
        console.log("Pass");
    }else console.log("Fail");
    // console.log(t1res);

}

async function concurrencyTest2(){
    let t2res;
    // db.setIsolationLevel(con1, 'read committed');
    await sleep(1000);
    let rank = 5 // 5 or null
    con1.beginTransaction((err)=>{
        if(err) throw err
        console.log("Transaction 1:")
        con1.query("UPDATE movies_all SET `rank`= "+ rank +" WHERE id=1", (err, res) => { // rank is null before
            // console.log(res);
            if(err) throw err
            con1.query("DO SLEEP(8)", (err) => {
                if(err) throw err
                con1.commit((err)=>{
                    console.log("Transaction 1 committed");
                    if(err) throw err
                    con3.query("UPDATE movies_post1980 SET `rank`= "+ rank +" WHERE id=1", (err) => {
                        if(err) throw err
                    })
                })
        
            })
        });

        // sleep(5000);

    });
    await sleep(1000);
    con1Clone.beginTransaction((err) => {
        if(err) throw err
        console.log("Transaction 2:");
        con1Clone.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE id = 1;", (err, res)=> {
            if(err) throw err

            con1Clone.commit((err)=> {
                console.log("Transaction 2 committed");
                t2res = res[0].rank;
                if(err) throw err
            })
        })

    })
    await sleep(9000);
    // T2 should be unupdated
    if(t2res == rank){
        // console.log(t2res);
        console.log('fail');
    }else{
        console.log('pass');
    }
    await sleep(1000);
    con3.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_post1980 WHERE id = 1;",(err, res) => {
        if(err) throw err

       if(res[0].rank == rank) console.log("node3 updated");
    })


}

async function concurrencyTest3(){
    let rank = 5 // null or 5
    // let year = 2000  // id 1; year 2000 is original
    con1.beginTransaction((err) => {
        if(err) throw err
        console.log("T1 Started");
        con1.query("UPDATE movies_all SET `rank`= "+ rank +" WHERE id=1", (err, res) => { // rank is null before
            // console.log(res);
            if(err) throw err
            con1.query("DO SLEEP(8)", (err) => {
                if(err) throw err
                con1.commit((err)=>{
                    console.log("Transaction 1 committed");
                    if(err) throw err
                    con3.query("UPDATE movies_post1980 SET `rank`= "+ rank +" WHERE id=1", (err) => {
                        if(err) throw err
                    })
                })
        
            })
        });
    })
    await sleep(1000);
    con1Clone.beginTransaction((err) => {
        if(err) throw err
        console.log("T2 Started");
        con1Clone.query("UPDATE movies_all SET `rank`= `rank` + 1 WHERE id=1", (err, res) => { // rank is null before
            // console.log(res);
            if(err) throw err
            con1Clone.commit((err)=>{
                console.log("Transaction 2 committed");
                if(err) throw err
                con3.query("UPDATE movies_post1980 SET `rank`= `rank` + 1 WHERE id=1", (err) => {
                    if(err) throw err
                })
            })
        });
    })
    await sleep(9000);
    con1.query("SELECT `id`, `name`, `year`, `rank`, genre, director FROM movies_all WHERE id = 1;", (err, res) => {
        if(res[0].rank == rank + 1){
            console.log("pass");
        }else{
            console.log("fail");
        }

    })
}

function sleep(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
async function runAllTests(){
    // await concurrencyTest1();
    // await concurrencyTest2();
    await concurrencyTest3();
}

runAllTests();
