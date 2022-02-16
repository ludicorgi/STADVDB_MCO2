const connections = require('./db_connections');
const db = require('./db_functions');

const insert = db.reallyNewInsert;
const insertFailPrimary = db.InsertSimulatePrimaryError;
const insertFailReplica = db.InsertSimulateReplicaError;
const recover = db.recoverAll;

const con1 = connections.con1;
const con2 = connections.con2;
const con3 = connections.con3;

async function runAllTests(){
    recover();
    console.log("done");
}

runAllTests();