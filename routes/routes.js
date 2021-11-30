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
    if (req.session.username) {
        //user is logged in, should be sent to the homepage
        //sending with username so homepage can be personalized to the logged in user
        res.render('home.ejs', { username: req.session.username });
    } else {
        //no user is logged in, should be sent to the login page
        res.redirect('/login');
    }
};

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
            error = err;
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
let signupError = ['Sign Up Information Incorrect'];
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
        }
    });
};

var getAccount = function (req, res) {
    if (!req.session.username) {
        res.redirect('/login?error=0');
    } else {
        res.render('account.ejs');
    }
};

var editAccount = function (req, res) {
    if (!req.session.username) {
        res.redirect('/login?error=0');
    } else {
        res.render('editaccount.ejs');
    }
};

var updateAccount = function (req, res) {
    db.update_user_profile(req.body, function (err, data) {
        if (err) {
            res.redirect('/editaccount?error=0');
        } else {
            res.redirect('/account');
        }
    });
};

var loadUserProfile = function (req, res) {
    console.log('Fetching User Profile Data');
    if (!req.session.username) {
        res.redirect('/login?error=0');
    } else {
        db.get_user_profile_data(req.session.username, function (err, data) {
            res.send(JSON.stringify(data));
        });
    }
};

/*
Fetches a specific subset of chats
Typical usecase is to get all chats that the logged in user is a part of
Can be expanded for other uses if needed
*/
var getChats = function (req, res) {
	if(!req.session.username) {
		//not logged in, redirect to login page
		res.redirect('/login?error=0');
	} else {
		db.get_users_chats(req.session.username, function(err, data) {
			if (err) {
				res.send(null);
			} else {
				res.send(JSON.stringify(data));
			}
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
	get_chats: getChats,
};

module.exports = routes;
