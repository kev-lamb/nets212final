var db = require('../models/database.js');

// A recurring variable that will render as an error 
// through ejs, detailing a user's mistake.
var error = '';

/**
Route to the homepage of the website.
If a user is logged in, should send them to the homepage.
If the user is not logged in, should send them to the login page
 */
var getHome = function(req, res) {
	if (req.session.username) {
		//user is logged in, should be sent to the homepage
		//sending with username so homepage can be personalized to the logged in user
		res.render('home.ejs', {user: req.session.username});
	} else {
		//no user is logged in, should be sent to the login page
		res.redirect('/login')
	}
};

/**
Route to the wall of a particular user. 
A request to this route should have the user_id of the user whose wall is being requested.
TODO: Implement
 */
var getWall = function(req, res) {
	
};

// Route to the login page
var getLogin = function(req, res) {
  req.session.destroy();
  res.render('login.ejs', {message:error});
  error = '';
};

// Route to the signup page, to register a new account
var getSignup = function(req, res) {
  req.session.destroy();
  res.render('signup.ejs', {message:error});
  error = '';
};

// Load the restaurants map after logging in 
var loadRest = function(req, res) {
  // The user must already be logged-in to access /restaurants!
  console.log("Loading the restaurant map.");
  if (!req.session.username) {
	error = 'You must log-in first!';
	res.redirect('/');
  }	else {
  db.scan_restaurants(function (err, data) {
	if (err) {
		console.log(err);
	} else {
		console.log("Successful scan of restaurants DB");
		let itemList = data;
		console.log(JSON.stringify(itemList));
		res.render('restaurantsmap.ejs', {DB_Items:itemList, Username:req.session.username, message:error});
  		error = '';
	}
  });	
  }
};

// Verify the provided login information, and redirect to the restaurant table page (if no error)
var loginCheck = function(req, res) {
  let username = req.body.username.trim();
  let password = req.body.password;

  db.login_check(username, password, function(err, data) {
    if (err) {
	  error = err;
      res.redirect('/');
    } else {
	  if (!req.session.username) {
		req.session.username = username;
	  }
      res.redirect('/restaurants');
    }
  });
};

// Register a new account, and redirect to the restaurant table page (if no error)
var newAccCheck = function(req, res) {
  let username = req.body.username.trim();
  let password = req.body.password;
  let fullname = req.body.fullname.trim();

  db.new_acc_check(username, password, fullname, function(err, data) {
    if (err) {
	  error = err;
      res.redirect('/signup');
    } else {	
	  if (!req.session.username) {
		req.session.username = username;
	  }
      res.redirect('/restaurants');
    }
  });
};

// Handles the three possible uses of POST /restaurants: 
// creating/deleting a restaurant or refreshing the list of restaurants to the client
var updateRest = function(req, res) {
  console.log("Rereshing restaurants.");
  if (req.body.name && req.body.latitude) {
// All fields will be non-null when adding a restaurant.
	newRest(req, res);
  } else if (req.body.name) {
// Only the req.body.name field will be non-null when deleting a restaurant.	
	delRest(req, res);
  }	else {
// No fields will be non-null when refreshing the list of restaurants.
	refRest(req, res);
  }
}

// Add a new restaurant to the restaurant table (if no error)
var newRest = function(req, res) {
  let rname = req.body.name.trim();
  let lat = req.body.latitude.trim();
  let long = req.body.longitude.trim();
  let desc = req.body.description.trim();
  //let creator = req.session.username; // Take in from req.body instead.
  let creator = req.body.creator.trim(); 

  res.type('text/plain');

  // No longer re-direct back to restaurants; that would re-load the page!
  // Instead, send back data through the res object.
  db.new_rest(rname, lat, long, desc, creator, function(err, data) {
    if (err) {
	  // error = err;
      // res.redirect('/restaurants');
      res.send(err); 
    } else {
      // res.redirect('/restaurants');
      res.send("");
    }
  });
};

// Removes a restaurant from the restaurant table (if no error)
var delRest = function(req, res) {
  let rname = req.body.name.trim();

  res.type('text/plain');

  db.delete_rest(rname, function(err, data) {
    if (err) {
      res.send(err); 
    } else {
      res.send("");
    }
  });
};

// Refreshes the list of restaurants known to the client
var refRest = function(req, res) {
  res.type('text/plain');
  db.scan_restaurants(function (err, data) {
    if (err) {
	  console.log(err);
      res.send("");
    } else {
	  console.log("Successful scan of restaurants DB");
	  let itemList = JSON.stringify(data);
	  console.log(itemList);
      res.send(itemList);
    }
  });	
}

// Logout the user, and return to the login page
var logout = function(req, res) {
  req.session.destroy();
  res.redirect('/');
};

var routes = { 
  get_login: getLogin,
  get_signup: getSignup,
  load_restaurants: loadRest,
  login_check: loginCheck,
  new_account_check: newAccCheck,
  update_restaurants: updateRest,
  new_restaurant: newRest,
  delete_restaurant: delRest,
  refresh_restaurant: refRest,
  logout: logout,
  get_home: getHome,
  get_wall: getWall
};

module.exports = routes;
