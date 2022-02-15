const db = require('../db_dummy/db_functions');
const search = db.newSearch;
const update = db.reallyNewUpdate;
const insert = db.reallyNewInsert;
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
        let data = {...req.body}
        let old_data = data.old_data;
        // name year rank genre director
        // console.log(data);
        // update()
        update(data.name, data.year, data.rank, data.genre, data.director,
                old_data[0], old_data[1], old_data[2], old_data[3], (result) => {
                    
                })
        res.status(200).send(data);
    }
};

module.exports = controller;