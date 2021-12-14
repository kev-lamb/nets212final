var db = require('../models/database.js');

// A recurring variable that will render as an error
// through ejs, detailing a user's mistake.
var error = '';

/**
Route to the homepage of the website.
If a user is logged in, should send them to the homepage.
If the user is not logged in, should send them to the login page
 */

var getHome = function (req, res) {
    loginProtectedRoute(req, res, () => {
        res.render('home.ejs', {
            username: req.session.username,
        });
    });
};

var getWall = function (req, res) {
    loginProtectedRoute(req, res, () => {
        db.get_posts_w(req.params.user, function (err, data) {
            if (err) {
                console.log(err);
            } else {
                res.render('wall.ejs', {
                    username: req.session.username,
                    walluser: req.params.user,
                    posts: data,

                });
			}
        });    
    });
};
/*if (req.session.username) {
        db.update_last_online(req.session.username);
        //user is logged in, should be sent to the homepage
        //sending with username so homepage can be personalized to the logged in user
        db.get_posts_hp(req.session.username, function(err, data) {
	 		if (err) {
				console.log(err);
			} else {
				res.render('home.ejs', {username: req.session.username, posts: data});
			}
		});
    } else {
        //no user is logged in, should be sent to the login page
        res.redirect('/login');
    }*/

// [login] index based url query custom error messages
let loginError = ['Login Information Incorrect'];
var getLogin = function (req, res) {
    res.render('login.ejs', { errorMessage: loginError[req.query.error] });
};

// Verify the provided login information, and redirect to the restaurant table page (if no error)
var loginCheck = function (req, res) {
    let username = req.body.username.trim();
    let password = req.body.password;

    db.login_check(username, password, function (err, data) {
        if (err) {
            res.redirect('/login?error=0');
        } else {
            if (!req.session.username) {
                req.session.username = username;
            }
            res.redirect('/');
        }
    });
};

// [signup] index based url query custom error messages
let signupError = ['Username taken/Sign Up Information Incorrect'];
var getSignup = function (req, res) {
    res.render('signup.ejs', { errorMessage: signupError[req.query.error] });
};

var newAccCheck = function (req, res) {
    db.new_acc_check(req.body, function (err, data) {
        if (err) {
            res.redirect('/signup?error=0');
        } else {
            if (!req.session.username) {
                req.session.username = req.body.username;
            }
            res.redirect('/');
            for (let i = 1; i <= req.session.username.length; i++) {
                let sub = req.session.username.substring(0, i);
                db.post_partial_search(sub, req.session.username, function () {
                    if (err) {
                        console.log(err);
                    } else {
                    }
                });
            }
        }
    });
};

var getAccount = function (req, res) {
    loginProtectedRoute(req, res, () => {
        res.render('account.ejs');
    });
};

var editAccount = function (req, res) {
    loginProtectedRoute(req, res, () => {
        res.render('editaccount.ejs');
    });
};

var updateAccount = function (req, res) {
    loginProtectedRoute(req, res, () => {
        db.update_user_profile(req.body, function (err, data) {
            if (err) {
                res.redirect('/editaccount?error=0');
            } else {
                res.redirect('/account');
            }
        });
    });
};

var loadUserProfile = function (req, res) {
    loginProtectedRoute(req, res, () => {
        db.get_user_profile_data(req.session.username, function (err, data) {
            res.send(JSON.stringify(data));
        });
    });
};

var searchForUsers = function (req, res) {
    loginProtectedRoute(req, res, () => {
        res.render('search.ejs');
    });
};

var loadPartialUserList = function (req, res) {
    loginProtectedRoute(req, res, () => {
        db.get_partial_users(req.body.searchTerm, function (err, data) {
            res.send(JSON.stringify(data));
        });
    });
};

var getFriends = function (req, res) {
    loginProtectedRoute(req, res, () => {
        res.render('friends.ejs');
    });
};

var last_online_user = function (req, res) {
    loginProtectedRoute(req, res, () => {
        db.check_last_online(req.params.user, (err, data) => {
            res.send({ ...data, user: req.params.user });
        });
    });
};

/*
Fetches a specific subset of chats
Typical usecase is to get all chats that the logged in user is a part of
Can be expanded for other uses if needed
*/
var getChats = function (req, res) {
    if (!req.session.username) {
        //not logged in, redirect to login page
        res.redirect('/login?error=0');
    } else {
        db.get_users_chats(req.session.username, function (err, data) {
            if (err) {
                res.send(null);
            } else {
                res.send(JSON.stringify(data));
            }
        });
    }
};

var signout = function (req, res) {
    req.session.username = '';
    res.redirect('/');
};

var testChats = function (req, res) {
    res.render('chats.ejs');
};

var openChat = function (req, res) {
    if (!req.session.username) {
        //users need to be logged in to access chats
        res.redirect('/login?error=0');
    } else {
        //if no chat id is specified in the query, redirect to the page of all users chats
        if (!req.query.chatid) {
            res.redirect('/testchat');
        } else {
            //check if the user is in the chat
            db.in_chat(
                req.session.username,
                req.query.chatid,
                function (err, data) {
                    if (data) {
                        //user is in the group, fetch the messages from this group and display them
                        db.get_chat(req.query.chatid, function (err, data) {

							db.get_title(req.query.chatid, function(err, title) {
                            	res.render('chat.ejs', {
                                	messages: data,
                                	chatid: req.query.chatid,
									username: req.session.username,
									title: title["chatName"].S
                            	});
							})
                        });
                    } else {
                        //user is not in the group/doesnt have permission, so redirect to the list of their chats
                        res.redirect('/testchat');
                    }
                }
            );
        }
    }
};

var getMessages = function (req, res) {
    if (!req.query.chatid) {
        res.send(null);
    } else {
        db.get_chat(req.query.chatid, function (err, data) {
            res.send(data);
        });
    }
};

var getUserPage = function (req, res) {
    loginProtectedRoute(req, res, () => {
        if (req.params.user == 'mywall') {
            req.params.user = req.session.username;
        }
        db.check_valid_user(req.params.user, function (err, data) {
            if (err || data.Count == 0) {
                res.redirect('/');
            } else {
                res.render('userpage.ejs', {
                    me: req.session.username,
                    person: req.params.user,
                    mywall: req.params.user == req.session.username,
                });
            }
        });
    });
};

var checkFriendStatus = function (req, res) {
    db.check_friend_status(
        req.session.username,
        req.body.user,
        function (err, data) {
            res.send(data);
        }
    );
};

var add_friend = function (req, res) {
    db.add_friend(req.session.username, req.body.user, function (err, data) {
        res.send();
    });
};

var remove_friend = function (req, res) {
    db.remove_friend(req.session.username, req.body.user, function (err, data) {
        res.send();
    });
};

var get_friends = function (req, res) {
    db.get_friends(req.session.username, function (err, data) {
        res.send(data);
    });
};

var get_visualizer_page = function (req, res) {
    loginProtectedRoute(req, res, () => {
        res.render('friendvisualizer.ejs');
    });
};

var get_visualizer_data = function (req, res) {
    db.get_friends_visualizer(
        req.session.username,
        req.params.nodeid,
        function (err, data) {
            res.send(data);
        }
    );
};

var sendMessage = function (req, res) {
    if (!req.session.username) {
        res.redirect('/login?error=0');
    } else {
        db.send_message(
            req.body.chatid,
            req.body.message,
            req.session.username,
            function (err, data) {
                if (err) {
                    res.send(null);
                } else {
                    res.send('success');
                }
            }
        );
    }
};

var post_a_post = function (req, res) {
    req.body.username = req.session.username;
    loginProtectedRoute(req, res, () => {
        db.post_a_post(req.body, function (err, data) {
            res.send(data);
        });
    });
};
var get_posts = function (req, res) {
    loginProtectedRoute(req, res, () => {
        db.get_posts_by_user(req.query.username, function (err, data) {
            res.send(data);
        });
    });
};

var get_wall_posts = function (req, res) {
    loginProtectedRoute(req, res, () => {
        db.get_posts_to_wall(req.query.wall, function (err, data) {
            res.send(data);
        });
    });
};

var delete_post = function (req, res) {
    loginProtectedRoute(req, res, () => {
        if (req.session.username != req.body.username) {
            res.send('Failed');
            return;
        }
        db.delete_post(req.body, function (err, data) {
            res.send(data);
        });
    });
};
/*
    Wraps all routes that require login so they always redirect to same place
    Update last online time
*/
var loginProtectedRoute = function (req, res, callback) {
    if (req.session.username) {
        db.update_last_online(req.session.username);
        callback();
    } else {
        res.redirect('/login?error=0');
    }
};

var newChat = function (req, res) {
	if(!req.session.username) {
		res.redirect('/login?error=0');
	} else {
		res.render('newchat.ejs', {username: req.session.username});
	}
};

var createChat = function (req, res) {
	if(!req.session.username) {
		res.redirect('/login?error=0');
	} else {
		var members = [];
		members.push(req.session.username);
		console.log(req.body);
		for(item in req.body) {
			if(item != 'title') {
				members.push(item);
			}
			console.log(item);
		}
		db.create_chat(req.body.title, members, function(err, data) {
			res.redirect("/testchat");
		});
	}
};

var yourfriends = function (req, res) {
	if(!req.session.username) {
		res.redirect('/login?error=0');
	} else {
		//fetch all of the current users friends
		db.get_friends(req.session.username, function (err, data) {
        	res.send(data);
    	});
		
	}
};

var get_title = function (req, res) {
	if(!req.session.username) {
		res.redirect('/login?error=0');
	} else {
		console.log(req.params);
		db.get_title(req.params.chatid, function(err, data) {
			console.log(data);
			res.send(data);
		});
	}
};

var routes = {
    home_page: getHome,
    login_page: getLogin,
    signup_page: getSignup,
    new_account_check: newAccCheck,
    login_check: loginCheck,
    user_account: getAccount,
    edit_user_account: editAccount,
    update_account_check: updateAccount,
    get_user_profile: loadUserProfile,
    wall: getWall,
    get_chats: getChats,
    test_chats: testChats,
    get_chat: openChat,
    get_messages: getMessages,
    send_message: sendMessage,
    get_search_users: searchForUsers,
    get_partial_search_users: loadPartialUserList,
    friends_page: getFriends,
    user_page: getUserPage,
    checkfriendstatus: checkFriendStatus,
    add_friend: add_friend,
    remove_friend: remove_friend,
    get_friends: get_friends,
    last_online_user: last_online_user,
    signout: signout,
    get_visualizer_page: get_visualizer_page,
    get_visualizer_data: get_visualizer_data,
    post_a_post: post_a_post,
    get_posts: get_posts,
    delete_post: delete_post,
    get_wall_posts: get_wall_posts,
	new_chat: newChat,
	create_chat: createChat,
	yourfriends: yourfriends,
	get_title: get_title,
};

module.exports = routes;
