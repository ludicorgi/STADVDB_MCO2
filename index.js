const dotenv = require('dotenv');
var express = require('express');
var app = express();
const bodyParser = require('body-parser');
const routes = require(`./routes/routes.js`);

dotenv.config();
port = process.env.PORT;
hostname = process.env.HOSTNAME;

app.use(bodyParser.urlencoded({ extended: false }));
app.use(express.urlencoded({extended: true}));

app.engine('html', require('ejs').renderFile);
app.set('view engine', 'html');

app.use(express.static('public'));

app.use(`/`, routes);
app.listen(port, function(){
    console.log(`Server running at:`);
    console.log(`http://` + hostname + `:` + port);
});