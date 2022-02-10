var mysql = require('mysql');

var con = mysql.createConnection({
  host: "localhost",
  port: 3310,
  user: "root",
  password: "12345",
  database: "testdb"
});

var con2 = mysql.createConnection({
    host: "moves-all.cxtjrbb21bon.ap-southeast-1.rds.amazonaws.com",
    port: 3306,
    user: "admin",
    password: "12345678",
    database: "node1"
});

function closeConnection(con){
    con.end(function(err){
        if (err) throw err;
        console.log("Closed connection");
    });
}

function printOneResult(results, fields){
    results.forEach(result => {
        fields.forEach(field => {
            process.stdout.write(result[field.name] + " ");
        });
        console.log();
    });
}

function printAllResults(results, fields){
    results.forEach((result, index) => {
        process.stdout.write("Result #" + index + ": ");
        fields.forEach(field => {
            process.stdout.write(result[field.name] + " ");
        })
        console.log();
    });
}

function executeQuery(con, sql){
    con.connect(function(err) {
        if (err) throw err;
        console.log("Connected!");

        con.query(sql, function(err, result, fields){
            if(err) throw err;
            // printAllResults(result, fields);
            closeConnection(con);
        });
    });
}

function selectAccountById(connection, condition){
    connection.connect(function(err){
        if(err) throw err;
        console.log("Connected!");

        con.query("SELECT * FROM accounts WHERE id=?", [condition], function(err, result, fields){
            if(err) throw err;
            printOneResult(result, fields);
            // console.log(result);
            closeConnection(con);
        });
    })
}

function insertOneRecordIntoAccounts(connection, values){
    connection.connect(function(err){
        if(err) throw err;
        console.log("Connected!");

        con.query("INSERT INTO accounts (id, amount) VALUES (?,?)", values, function(err, result){
            if(err) throw err;
            console.log("1 record inserted");
            console.log(result);
            closeConnection(con);
        });
    })
}

function insertManyRecordsIntoAccounts(connection, values){
    connection.connect(function(err){
        if(err) throw err;
        console.log("Connected!");

        con.query("INSERT INTO accounts (id, amount) VALUES ?", [values], function(err, result){
            if(err) throw err;
            console.log("Number of records inserted: " + result.affectedRows);
            closeConnection(con);
        });
    })
}

function updateAmountForOneRecordFromAmounts(connection, condition, value){
    connection.connect(function(err){
        if(err) throw err;
        console.log("Connected!");

        con.query("UPDATE accounts SET amount = ? WHERE id = ?", [value, condition], function(err, result){
            if(err) throw err;
            console.log("Number of records updated: " + result.affectedRows);
            closeConnection(con);
        });
    })
}

var sql = "ALTER TABLE movies_all MODIFY COLUMN id INT auto_increment";

executeQuery(con2, sql);

// con2.connect(function(err) {
//     if (err) throw err;
//     console.log("Connected!");

//     con2.query("SELECT * FROM movies_all", function(err, result, fields){
//         if(err) throw err;
//         printAllResults(result, fields);
//         closeConnection(con2);
//     });
// });

// con2.connect(function(err) {
//     if (err) throw err;
//     console.log("Connected!");

//     con2.query("SHOW INDEX FROM movies_all", function(err, result, fields){
//         if(err) throw err;
//         printAllResults(result, fields);
//         closeConnection(con2);
//     });
// });

// con2.connect(function(err) {
//     if (err) throw err;
//     console.log("Connected!");

//     con2.query("DROP DATABASE mco1dw", function(err, result, fields){
//         if(err) throw err;
//         closeConnection(con2);
//     });
// });

// executeQuery(con, sql);
// selectAccountById(con, 2);
insertOneRecordIntoAccounts(con, [19,123]);

// var values = [[13,213], [14,237], [15,250]];
// insertManyRecordsIntoAccounts(con, values);

// updateAmountForOneRecordFromAmounts(con, 15, 255);