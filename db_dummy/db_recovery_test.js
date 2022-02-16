const connections = require('./db_connections');
const db = require('./db_functions');

const insert = db.reallyNewInsert;
const insertFailPrimary = db.InsertSimulatePrimaryError;
const insertFailReplica = db.InsertSimulateReplicaError;
const recover = db.recoverAll;

const con1 = connections.con1;
const con2 = connections.con2;
const con3 = connections.con3;

var name = "TestMovieTestDB2";
var name2 = "TestMovieTestDB3";
    var year = 1975;
    var rank = 0;
    var genre = "Action";
    var director = "John Brian";

async function recovTest1(){

    insertFailPrimary(name, year, rank, genre, director, (result) => {});
    recover();

    setTimeout(function () {
        con1.query("SELECT * from final_movies_all WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err, results){
            console.log("Node1 copy");
            console.log(results);
        })
    
        con2.query("SELECT * from final_movies_pre1980 WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err, results){
            console.log("Node2 copy");
            console.log(results);
        })
    
        con3.query("SELECT * from final_movies_post1980 WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err, results){
            console.log("Node3 copy");
            console.log(results);
        })

        setTimeout(function () {
            con1.query("DELETE FROM final_movies_all WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err){});
            con2.query("DELETE FROM final_movies_pre1980 WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err){});
            con3.query("DELETE FROM final_movies_post1980 WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err){});
            console.log("done");
        }, 2000);
        
    }, 3000);

}

async function recovTest2(){
    insert(name2, year, rank, genre, director, (result) => {});
    insertFailReplica(name, year, rank, genre, director, (result) => {});

    setTimeout(function () {
        recover();
    }, 2000);

    setTimeout(function () {
        con1.query("SELECT * from final_movies_all WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err, results){
            console.log("Node1 copy");
            console.log(results);
        })
    
        con2.query("SELECT * from final_movies_pre1980 WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err, results){
            console.log("Node2 copy");
            console.log(results);
        })
    
        con3.query("SELECT * from final_movies_post1980 WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err, results){
            console.log("Node3 copy");
            console.log(results);
        })

        setTimeout(function () {
            con1.query("DELETE FROM final_movies_all WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err){});
            con2.query("DELETE FROM final_movies_pre1980 WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err){});
            con3.query("DELETE FROM final_movies_post1980 WHERE name=? AND year=? AND `rank`=? AND genre=? AND director=?;", [name, year, rank, genre, director], function(err){});
            console.log("done");
        }, 2000);
        
    }, 4000);
}

async function runTest(){
    recovTest3();
}

runTest();