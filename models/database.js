var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
const bcrypt = require('bcrypt');
const crypto = require('crypto');

// Verify that the provided login information matches an item in the users table
var myDB_login_check = function (inputUname, inputPword, callback) {
    console.log(
        'Login Check: Querying for username: ' +
            inputUname +
            ', password: ' +
            inputPword
    );

    let params = {
        TableName: 'users',
        KeyConditionExpression: '#un = :uname',
        ExpressionAttributeNames: {
            '#un': 'username',
        },
        ExpressionAttributeValues: {
            ':uname': {
                S: inputUname,
            },
        },
    };

    db.query(params, function (err, data) {
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
                    bcrypt
                        .compare(inputPword, pword)
                        .then((res) => {
                            if (!res) {
                                callback('Incorrect password!', null);
                            } else {
                                callback(null, data.Items[0]);
                            }
                        })
                        .catch((err) => console.error(err.message));
                }
            }
        }
    });
};

// Add a new item to the users table if the input username does not already exist
var myDB_new_acc_check = function (inputData, callback) {
    console.log(
        'New Acc Check: Querying for username: ' +
            inputData.username +
            ', password: ' +
            inputData.password +
            ', first name: ' +
            inputData.firstname
    );

    let params = {
        TableName: 'users',
        KeyConditionExpression: '#un = :uname',
        ExpressionAttributeNames: {
            '#un': 'username',
        },
        ExpressionAttributeValues: {
            ':uname': {
                S: inputData.username,
            },
        },
    };
    const saltRounds = 10;
    bcrypt
        .hash(inputData.password, saltRounds)
        .then((hash) => {
            db.query(params, function (err, data) {
                if (err) {
                    callback(err, null);
                } else {
                    if (data.Items.length > 0) {
                        callback('User already existed!', null);
                    } else {
                        let params = {
                            TableName: 'users',
                            Item: {
                                username: {
                                    S: inputData.username,
                                },
                                password: {
                                    S: hash,
                                },
                                firstname: {
                                    S: inputData.firstname,
                                },
                                lastname: {
                                    S: inputData.lastname,
                                },
                                email: {
                                    S: inputData.email,
                                },
                                affiliation: {
                                    S: inputData.affiliation,
                                },
                                birthday: {
                                    S: inputData.birthday,
                                },
                            },
                        };
                        // Add the new item to the users table
                        db.putItem(params, function (err, data) {
                            if (err) {
                                callback(
                                    'Unable to add item. Error JSON:',
                                    JSON.stringify(err, null, 2),
                                    null
                                );
                            } else {
                                callback(null, 'New account created.');
                            }
                        });
                    }
                }
            });
        })
        .catch((err) => console.error(err.message));
};

var myDB_data_user_profile = function (username, callback) {
    var params = {
        TableName: 'users',
        Key: {
            username: {
                S: username,
            },
        },
    };
    db.getItem(params, function (err, data) {
        callback(err, data);
    });
};

var update_user_profile = function (inputData, callback) {
    let params = {};
    console.log(inputData);
    if (inputData.password) {
        console.log('In data');
        const saltRounds = 10;
        bcrypt.hash(inputData.password, saltRounds).then((hash) => {
            params = {
                TableName: 'users',
                Key: {
                    username: {
                        S: inputData.username,
                    },
                },
                UpdateExpression: 'set password = :password',
                ExpressionAttributeValues: {
                    ':password': { S: hash },
                },
                ReturnValues: 'UPDATED_NEW',
            };
            db.updateItem(params, function (err, data) {
                if (err) {
                    console.log(err);
                }
                callback(err, data);
            });
        });
    } else {
        params = {
            TableName: 'users',
            Key: {
                username: {
                    S: inputData.username,
                },
            },
            UpdateExpression:
                'set ' +
                'firstname = :firstname, ' +
                'lastname = :lastname, ' +
                'email = :email,' +
                'affiliation = :affiliation, ' +
                'birthday = :birthday',
            ExpressionAttributeValues: {
                ':firstname': { S: inputData.firstname },
                ':lastname': { S: inputData.lastname },
                ':email': { S: inputData.email },
                ':affiliation': { S: inputData.affiliation },
                ':birthday': { S: inputData.birthday },
            },
            ReturnValues: 'UPDATED_NEW',
        };
        db.updateItem(params, function (err, data) {
            if (err) {
                console.log(err);
            }
            callback(err, data);
        });
    }
};

var myDB_get_posts_wall = function(user, callback) {
  console.log('Beginning post table scan.'); 

  //get posts to user
  params = {
	TableName : "posts",
	KeyConditionExpression: 'postee = :user',
	ExpressionAttributeValues: {
	  ':user' : {'S': user}
	}
  }

  db.query(params, function(err, data) {
	if (err) {
		callback(err, null);
	} else {
		for (var i = 0; i < data.length; i++) {
			if (data.Items[i].poster.S != data.Items[i].postee.S) {
				out.push(data.Items[i]);
			}
		}
	}
  });

  //get posts from user
  params = {
	TableName : "posts",
	IndexName : "poster-postid-index",
	KeyConditionExpression: 'poster = :user',
	ExpressionAttributeValues: {
	  ':user' : {'S': user}
	}
  }

  db.query(params, function(err, data) {
	if (err) {
		callback(err, null);
	} else {
		for (var i = 0; i < data.Items.length; i++) {
			out.push(data.Items[j]);
		}
	}
  });

  callback(null, out);
}

var myDB_get_posts_homepage = function(user, callback) {
  console.log('Beginning post table scan.'); 

  var friends = [];
  var out = [];  

  //get users friends
  let params = {
	TableName: "friends",
	KeyConditionExpression: "user = :user",
	ExpressionAttributeValues: {
		':user' : {'S': user}
	}
  }

  db.query(params, function(err, data) {
	if (err) {
		callback(err, null);
	} else {
		var friends = [];
		for (var i = 0; i < data.Items.length; i++) {
			params = {
				TableName: "posts",
				KeyConditionExpression: "postee = :user",
				ExpressionAttributeValues: {
					':user' : {'S': data.Items[i].friend.S}
				}
			}
			db.query(params, function(err, data2) {
			  if (err) {
		  		  callback(err, null);
			  } else {
				  for (var j = 0; j < data2.Items.length; j++) {
					if (data2.Items[j].poster.S == data.Items[i].friend.S && data2.items[j].postee != user) {
						out.push(data2.Items[j]);
					}
				  }
			  }
		    });
		}
	}
  });

  //get posts to user
  params = {
	TableName : "posts",
	KeyConditionExpression: 'postee = :user',
	ExpressionAttributeValues: {
	  ':user' : {'S': user}
	}
  }

  db.query(params, function(err, data) {
	if (err) {
		callback(err, null);
	} else {
		for (var i = 0; i < data.length; i++) {
			if (data.Items[i].poster.S != data.Items[i].postee.S) {
				out.push(data.Items[i]);
			}
		}
	}
  });

  //get posts from user
  params = {
	TableName : "posts",
	IndexName : "poster-postid-index",
	KeyConditionExpression: 'poster = :user',
	ExpressionAttributeValues: {
	  ':user' : {'S': user}
	}
  }

  db.query(params, function(err, data) {
	if (err) {
		callback(err, null);
	} else {
		for (var i = 0; i < data.Items.length; i++) {
			out.push(data.Items[j]);
		}
	}
  });

  callback(null, out);
}

var myDB_post = function (title, content, poster, postee, callback) {
	let current = new Date();
	let cDate = current.getFullYear() + '-' + (current.getMonth() + 1) + '-' + current.getDate();
	let cTime = current.getHours() + ":" + current.getMinutes() + ":" + current.getSeconds();
	let dateTime = cDate + ' ' + cTime;
	let params = {
                            TableName: 'posts',
                            Item: {
                                title: {
                                    S: title,
                                },
                                content: {
                                    S: content,
                                },
                                poster: {
                                    S: poster,
                                },
                                postee: {
                                    S: postee,
                                },
                                time: {
									S: dateTime
								},
								postid: {
									S: poster + cTime
								}
                            },
                        };
	db.putItem(params, function(err, data) {
        if (err) {
            console.log(err);
        }
        callback(err, data);		
	});
}

var get_users_chats = function (user, callback) {
	console.log("looking for chats containing user");
	console.log(user);
	//get chats containing user
  	params = {
		TableName : "chats",
		IndexName : "sortkey-chatID-index",
		KeyConditionExpression: 'sortkey = :user',
		ExpressionAttributeValues: {
	  		':user' : {'S': 'member_' + user}
		}
  	}

	db.query(params, function(err, data) {
		if (err) {
			callback(err, null);
		} else {
			console.log("found chats");
			console.log(data);
			callback(err, data.Items);
		}
  	});	
};

/**
Searches the Chats table for specific chat and checks if the given user is in the chat.
If the user is in the chat, data contains TRUE. Otherwise, data contains FALSE */
var in_chat = function(user, chatid, callback) {
	console.log("checking if user "+user+" has access to chat with id "+chatid);
	params = {
		TableName : "chats",
		KeyConditionExpression: 'chatID = :chatid and sortkey = :sortkey',
		ExpressionAttributeValues: {
	  		':chatid' : {'S': chatid},
			':sortkey' : {'S': 'member_'+user}
		}
  	};
	
	db.query(params, function(err, data) {
		if(err) {
			console.log(err);
		}
		callback(err, data);
	});
};

var get_chat = function(chatid, callback) {
	console.log("retreiving message from chat with id "+chatid);
	params = {
		TableName : "chats",
		KeyConditionExpression: 'chatID = :chatid and begins_with(sortkey, :msg)',
		ExpressionAttributeValues: {
	  		':chatid' : {'S': chatid},
			':msg' : {'S': 'message'}
		}
  	};

	db.query(params, function(err, data) {
		if(err) {
			console.log(err);
		}
		console.log("no errors");
		console.log(data);
		callback(err, data);
	})
};

/**
Puts an item into the chats table corresponding to <message> being sent by <user> in chat <chatid>
The sortkey for a message in the chats table is of the form message_<timestamp>_<messageid>.
We will produce the timestamp using javascripts native methods, and produce a unique message id
using nodes native crypto module. The message id doesnt have to be super long as itll only be needed
in the rare case that two messages are sent by different users at the exact same time.
 */
var send_message = function(chatid, message, user, callback) {
	console.log("adding new message to chat "+chatid);
	//get the timestamp
	var today = new Date();
	var timestamp = today.getFullYear()+'-'+(today.getMonth()+1)+'-'+today.getDate()+'-'
					+today.getHours() + ":" + today.getMinutes() + ":" + today.getSeconds();
	var messageid = crypto.randomBytes(16).toString("hex").slice(24);
	var params = {
      Item: {
        "chatID": {
          S: chatid
        },
        "sortkey": { 
          S: 'message_'+timestamp+'_'+messageid
        },
		"createdAt": {
		  S: timestamp
		},
		"username": {
		  S: user
		},
		"message": {
		  S: message
		}
      },
      TableName: "chats",
      ReturnValues: 'NONE'
  	};

	//put the given item into the table
	db.putItem(params, function(err, data){
    	if (err) {
      		callback(err);
		} else {
			callback(null, 'Success');
		}
  	});
};

var database = { 
  login_check: myDB_login_check,
  new_acc_check: myDB_new_acc_check,
  get_user_profile_data: myDB_data_user_profile,
  update_user_profile: update_user_profile,
  get_posts_w: myDB_get_posts_wall,
  get_posts_hp: myDB_get_posts_homepage,
  new_post: myDB_post,
  get_users_chats: get_users_chats,
  in_chat: in_chat,
  get_chat: get_chat,
  send_message: send_message,
};

module.exports = database;
                                        
