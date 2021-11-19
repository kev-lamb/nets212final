var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();

// Verify that the provided login information matches an item in the users table
var myDB_login_check = function(inputUname, inputPword, callback) {
  console.log('Login Check: Querying for username: ' + inputUname + ', password: '+ inputPword); 

  let params = {
	TableName : "users",
    KeyConditionExpression: "#un = :uname",
	ExpressionAttributeNames:{
    	"#un": "username"
	},
    ExpressionAttributeValues: {
        ":uname": {
			S: inputUname
		}
    }
  };

  db.query(params, function(err, data) {
	if (inputUname.length == 0 || inputPword.length == 0) {
		callback('Complete all inputs!', null);
	} else {
		if (err) {
			callback(err, null);
		} else {
			if (data.Items.length == 0) {
			  callback('User does not exist!', null);
			} else {
				// The input password must match the actual password of the user!
			  	let pword = data.Items[0].password.S;
				if (inputPword !== pword) {
					callback('Incorrect password!', null);
			  	} else {
					callback(null, data.Items[0]);
			  	}
			}
		}
	}
  });
}

// Add a new item to the users table if the input username does not already exist
var myDB_new_acc_check = function(inputUname, inputPword, inputFname, callback) {
  console.log('New Acc Check: Querying for username: ' + inputUname +
 ', password: '+ inputPword + ', full name: ' + inputFname); 

  let params = {
	TableName : "users",
    KeyConditionExpression: "#un = :uname",
	ExpressionAttributeNames:{
    	"#un": "username"
	},
    ExpressionAttributeValues: {
        ":uname": {
			S: inputUname
		}
    }
  };

  db.query(params, function(err, data) {
	if (inputUname.length == 0 || inputPword.length == 0 || inputFname.length == 0) {
		callback('Complete all inputs!', null);
	} else {
		if (err) {
			callback(err, null);
		} else {
			if (data.Items.length > 0) {
			  callback('User already existed!', null);
			} else {
				// We are now free to add the new item to the users table
				let params = {
				    TableName: "users",
				    Item:{
				        "username": {
							S: inputUname					
						},
				        "password": {
							S: inputPword
						},
				        "fullname": {
							S: inputFname
						}
				    }
				};
				
				// Add the new item to the users table
				db.putItem(params, function(err, data) {
				    if (err) {
						callback("Unable to add item. Error JSON:", JSON.stringify(err, null, 2), null);
				    } else {
				        console.log("Added item:", JSON.stringify(data, null, 2));
						callback(null, 'New account created.');
				    }
				});
			}
			
		}
	}
  });
}

// Retrieve all items from the restaurants table
var myDB_scan_restaurants = function(callback) {
  console.log('Beginning restaurant table scan.'); 

  let params = {
	TableName : "restaurants"
  };

  db.scan(params, function(err, data) {
	if (err) {
		callback(err, null);
	} else {
		callback(null, data.Items);
	}
  });
}

// Add a new restaurant to the restaurants table
var myDB_new_rest = function(inputRname, inputLat, inputLong, inputDesc, creator, callback) {
	if (!creator) {
		// We allow only signed-in users to modify the restaurants table
		callback('You must sign in first!', null);
	} else {
		if (inputRname.length == 0 || inputLat.length == 0 
			|| inputLong.length == 0 || inputDesc.length == 0) {
			callback('Complete all inputs!', null);		
		} else {
		console.log('New Rest Check with name: ' + inputRname +
		', latitude: '+ inputLat + ', longitude: ' + inputLong + 
		', description: '+ inputDesc + ', creator: ' + creator); 
		
		// We are now free to add the new item to the restaurants table
		let params = {
		    TableName: "restaurants",
		    Item:{
		        "name": {
					S: inputRname					
				},
		        "latitude": {
					S: inputLat
				},
		        "longitude": {
					S: inputLong
				},
				"description": {
					S: inputDesc
				},
		        "creator": {
					S: creator
				}
		    }
		};
		
		// Add the new item to the restaurants table
		db.putItem(params, function(err, data) {
		    if (err) {
				callback("Unable to add item. Error JSON:", JSON.stringify(err, null, 2), null);
		    } else {
		        console.log("Added item:", JSON.stringify(data, null, 2));
				callback(null, 'New restaurant created.');
		    }
		});
		}
	}
}

// Delete a restaurant from the restaurants table
var myDB_del_rest = function(inputRname, callback) {
	console.log('Deleting restaurant with name: ' + inputRname); 
	
	let params = {
	    TableName: "restaurants",
	    Key:{
	        "name": {
				S: inputRname					
			}
	    }
	};
	
	// Delete the item from the restaurants table
	db.deleteItem(params, function(err, data) {
	    if (err) {
			callback("Unable to delete item. Error JSON:", JSON.stringify(err, null, 2), null);
	    } else {
	        console.log("Deleted item:", JSON.stringify(data, null, 2));
			callback(null, 'Restaurant deleted.');
	    }
	});
		
	
}

var database = { 
  login_check: myDB_login_check,
  new_acc_check: myDB_new_acc_check,
  scan_restaurants: myDB_scan_restaurants,
  new_rest: myDB_new_rest,
  delete_rest: myDB_del_rest
};

module.exports = database;
                                        