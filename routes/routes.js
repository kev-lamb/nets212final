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
        if (req.session.username) {
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
    	}
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
                            res.render('chat.ejs', {
                                messages: data,
                                chatid: req.query.chatid,
                            });
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
        res.render('userpage.ejs', { person: req.params.user });
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

var sendMessage = function (req, res) {
    console.log('we are in the post function');
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
    get_chats: getChats,
    test_chats: testChats,
    get_chat: openChat,
    get_messages: getMessages,
    send_message: sendMessage,
    get_search_users: searchForUsers,
    get_partial_search_users: loadPartialUserList,
    get_chats: getChats,
    friends_page: getFriends,
    user_page: getUserPage,
    checkfriendstatus: checkFriendStatus,
    add_friend: add_friend,
    remove_friend: remove_friend,
    get_friends: get_friends,
    last_online_user: last_online_user,
    signout: signout,
};

module.exports = routes;
