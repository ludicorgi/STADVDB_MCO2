const db = require('../db/db_functions');
const demo = require('../db/db_concurrency_tests');
const search = db.newSearch;
const update = db.reallyNewUpdate;
const insert = db.reallyNewInsert;
const end = db.closeAllConnection;
const reports = db.generateAllReports;
const logs = demo.runAllTests;
const recover = db.recoverAll;

const controller = {
    getIndex: function(req, res) {
        res.render('index');
    },

    searchMovie: function(req, res){
        let data = {...req.query}
        // field and value
        // console.log(data);
        search(data.field, data.value, (result) => {
            res.status(200).send(result);
        })

        // db.searchMovie(data.field, data.value, (res) => {

        // })
    },

    addMovie: function(req, res){
        let data = {...req.body}
        // name year rank genre director
        // console.log(data);
        insert(data.name, data.year, data.rank, data.genre, data.director, (result) => {
            res.status(200).send(result);
        })

    },

    editMovie: function(req, res){
        let data = req.body
        // name year rank genre director
        console.log(data);
        update(data.name, data.year, data.rank, data.genre, data.director,
                data.o_name, data.o_year, data.o_genre, data.o_director, (result) => {
                    console.log(result);
                    if(result) res.status(200).send(result);
                    else res.status(500).send(false)
                })
    },

    endConnection: function(req, res){
        end();
        res.status(200);
    },

    generateReports: function(req, res){
        let data = req.query
        reports(data.genre, data.year, data.director, (results)=> {
            console.log(results);
            if(results) res.status(200).send(results)
            else res.status(500).send([0,0,0])
        })
    },

    demoLogs: function(req, res){
        logs((log)=> {
            // console.log(log);
            if(log){
                res.status(200).send(log);
            }else{
                res.status(500).send("");
            }
        })
    },

    recoverNodes: function(req, res){
        // console.log("here");
        recover();
    }
};

module.exports = controller;