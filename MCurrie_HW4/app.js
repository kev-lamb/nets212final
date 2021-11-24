/* Some initialization boilerplate. Also, we include the code from
   routes/routes.js, so we can have access to the routes. Note that
   we get back the object that is defined at the end of routes.js,
   and that we use the fields of that object (e.g., routes.get_main)
   to access the routes. */

var express = require('express');
var routes = require('./routes/routes.js');
var session = require('express-session');

var app = express();
app.use(express.urlencoded());
app.use(session({
  secret: 'a little secret',
  resave: false,
  saveUninitialized: true,
  cookie: { secure: false }
}))

/* Below we install the routes. The first argument is the URL that we
   are routing, and the second argument is the handler function that
   should be invoked when someone opens that URL. Note the difference
   between app.get and app.post; normal web requests are GETs, but
   POST is often used when submitting web forms ('method="post"'). */

//home page
app.get('/', routes.get_home);

//wall page (for particular user)
app.get('/wall', routes.get_wall);

// Log-in page
app.get('/login', routes.get_login);

// Log-in check
app.post('/checklogin', routes.login_check);

// Sign-up page
app.get('/signup', routes.get_signup);

// Account creation
app.post('/createaccount', routes.new_account_check);

// Restaurants page
app.get('/restaurants', routes.load_restaurants);

// Restaurant addition; for XMLHttpRequest, re-use the restaurants domain. (Same-origin policy.)
app.post('/restaurants', routes.update_restaurants);
// app.post('/addrestaurant', routes.new_restaurant);

// Log-out page
app.get('/logout', routes.logout);

/* Run the server */

console.log('Author: curriem');
app.listen(80);
//app.listen(8080);
console.log('Server running on port 80. Now open http://localhost:8080/ in your browser!');
