const db = require('../db_dummy/db_functions');
const controller = {
    getIndex: function(req, res) {
        res.render('index');
    },

    searchMovie: function(req, res){
        let data = {...req.query}
        console.log(data);
        res.status(200).send(data);
        // db.searchMovie(data.field, data.value, (res) => {

        // })
    },

    addMovie: function(req, res){
        let data = {...req.body}
        console.log(data);
        res.status(200).send(data);
    },

    editMovie: function(req, res){
        let data = {...req.body}
        console.log(data);
        res.status(200).send(data);
    }
};

module.exports = controller;