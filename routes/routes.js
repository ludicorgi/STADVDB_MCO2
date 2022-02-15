const express = require(`express`);

const controller = require(`../controllers/controller.js`);
const app = express();

app.get(`/`, controller.getIndex);

// Search Movies
app.get('/search_movie', controller.searchMovie);

// Add Movie
app.post('/add_movie', controller.addMovie);

// Edit Movie
app.post('/edit_movie', controller.editMovie);

// End Connection
app.get('/end_connections', controller.endConnection);



module.exports = app;