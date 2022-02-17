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
// app.get('/end_connections', controller.endConnection);

// Generate Reports
app.get('/generate_reports', controller.generateReports);

// Demo Logs
app.get('/demo1', controller.demoLogs1);
app.get('/demo2', controller.demoLogs2);
app.get('/demo3', controller.demoLogs3);

// Recovery
// app.get('/recover', controller.recoverNodes);


module.exports = app;