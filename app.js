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
app.use(express.static('css'));
var http = require('http').Server(app);
var io = require('socket.io')(http);

io.on('connection', (socket) => {
    console.log('a user connected');
	//send chats to all users in a particular chat when a messages is received server-side
    socket.on('send-chat-message', (data) => {
        console.log('server received a message');
        console.log(data.message);
        socket.broadcast.emit('chat-message', {
            message: data.message,
            user: data.username,
            chatid: data.chatid,
        });
    });

	//send chat invitation to a user when its received by the server
	socket.on('invite-user-to-chat', (data) => {
		socket.broadcast.emit('invite-user', {
			user_invited: data.user_invited,
			user_inviting: data.user_inviting,
			chatid: data.chatid,
			chat_title: data.chat_title
		});
	});

});

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

// wall page
app.post('/wall/:user', routes.wall);

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

//adds a user to a chat they were invited to
app.post('/joinchat', routes.join_chat);

//removes a user from a chat they wish to leave
app.get('/leave/:chatid', routes.leave_chat);

//post a message to the database
app.post('/sendmessage', routes.send_message);

// get a specific subset of chats
app.get('/chats', routes.get_chats);

app.get('/newchat', routes.new_chat);

//go to the page of a specific chat
app.get('/chat', routes.get_chat);

//dummy route to test chat functionality
app.get('/testchat', routes.test_chats);

// create a new chat
app.post('/createchat', routes.create_chat);

// get the title of a chat with a given id
app.get('/data/chat/titles/:chatid', routes.get_title);

//get all users in a specific chat
app.get('/data/users/:chatid', routes.get_chat_members);

// get search page
app.get('/search', routes.get_search_users);

// post partial search results
app.post('/partialsearch', routes.get_partial_search_users);

// post all search results
//app.post('/allsearch', routes.get_all_search_users);

// get friends
app.get('/friends', routes.friends_page);

// get user profile
app.get('/user/:user', routes.user_page);

// get signout
app.get('/signout', routes.signout);

//post friend status
app.post('/checkfriendstatus', routes.checkfriendstatus);

//post add friend
app.post('/addfriend', routes.add_friend);

//post remove friend
app.post('/removefriend', routes.remove_friend);

//post get all friends
app.post('/getfriends', routes.get_friends);

// get your friends
app.get('/yourfriends', routes.yourfriends);

//post get last online status
app.post('/lastonline/:user', routes.last_online_user);

//get visualizer page
app.get('/friendvisualizer', routes.get_visualizer_page);

//get visualizer data
app.get('/friendvisualizationdata/:nodeid', routes.get_visualizer_data);

//post a post!
app.post('/makeapost', routes.post_a_post);

//post a comment!
app.post('/makeacomment', routes.comment_a_comment);

//get user posts
app.get('/getposts', routes.get_posts);

//get comments on posts
app.get('/getcomments', routes.get_comments);

//get posts to wall
app.get('/getwallposts', routes.get_wall_posts);

//get user posts
app.post('/deletepost', routes.delete_post);

console.log('Author: G31');
http.listen(8080);
console.log(
    'Server running on port 80. Now open http://localhost:80/ in your browser!'
);
