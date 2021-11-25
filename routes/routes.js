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

var getLogin = function (req, res) {
    res.render('login.ejs');
};

// Verify the provided login information, and redirect to the restaurant table page (if no error)
var loginCheck = function (req, res) {
    let username = req.body.username.trim();
    let password = req.body.password;

    db.login_check(username, password, function (err, data) {
        if (err) {
            error = err;
            res.redirect('/');
        } else {
            if (!req.session.username) {
                req.session.username = username;
            }
            res.redirect('/');
        }
    });
};

var getSignup = function (req, res) {
    res.render('signup.ejs');
};

var newAccCheck = function (req, res) {
    let username = req.body.username.trim();
    let password = req.body.password;
    let fullname = req.body.fullname.trim();

    db.new_acc_check(username, password, fullname, function (err, data) {
        if (err) {
            error = err;
            res.redirect('/signup');
        } else {
            if (!req.session.username) {
                req.session.username = username;
            }
            res.redirect('/');
        }
    });
};

var routes = {
    home_page: getHome,
    login_page: getLogin,
    signup_page: getSignup,
    new_account_check: newAccCheck,
    login_check: loginCheck,
};

module.exports = routes;
