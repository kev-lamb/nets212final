var express = require('express');
var routes = require('./routes/routes.js');
var session = require('express-session');

var app = express();
app.use(express.urlencoded());
app.use(
    session({
        secret: 'a little secret',
        resave: false,
        saveUninitialized: true,
        cookie: { secure: false },
    })
);

// home page is first page we see
app.get('/', routes.home_page);

// login page
app.get('/login', routes.login_page);

// sign page
app.get('/signup', routes.signup_page);

// create account page
app.post('/createaccount', routes.new_account_check);

// check login page
app.post('/checklogin', routes.login_check);

console.log('Author: G31');
app.listen(8080);
console.log(
    'Server running on port 8080. Now open http://localhost:8080/ in your browser!'
);
