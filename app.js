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
app.use(express.static('scripts'));

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

// user account page
app.get('/account', routes.user_account);

// edit user account page
app.get('/editaccount', routes.edit_user_account);

// update user account db call
app.post('/updateaccount', routes.update_account_check);

// get user profile
app.get('/data/userprofile', routes.get_user_profile);

//get messages from a chat
app.get('/data/chat', routes.get_messages);

//post a message to the database
app.post('/sendmessage', routes.send_message);

// get a specific subset of chats
app.get('/chats', routes.get_chats);

//go to the page of a specific chat
app.get('/chat', routes.get_chat);

//dummy route to test chat functionality
app.get('/testchat', routes.test_chats);

// get search page
app.get('/search', routes.get_search_users);

// post partial search results
app.post('/partialsearch', routes.get_partial_search_users);

// post all search results
//app.post('/allsearch', routes.get_all_search_users);

// post signout
app.get('/signout', routes.signout);

console.log('Author: G31');
app.listen(8080);
console.log(
    'Server running on port 8080. Now open http://localhost:8080/ in your browser!'
);
