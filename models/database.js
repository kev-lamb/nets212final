var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-1' });
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

var check_valid_user = function (username, callback) {
    let params = {
        TableName: 'users',
        KeyConditionExpression: '#un = :uname',
        ExpressionAttributeNames: {
            '#un': 'username',
        },
        ExpressionAttributeValues: {
            ':uname': {
                S: username,
            },
        },
    };
    db.query(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
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
    if (inputData.password) {
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

var myDB_get_posts_wall = function (username, callback) {
    console.log('Beginning post table scan.');

    //get posts to user
    params = {
        TableName: 'posts',
        KeyConditionExpression: 'postee = :username',
        ExpressionAttributeValues: {
            ':username': { S: username },
        },
        FilterExpression: 'poster <> :username',
    };

    db.query(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            out = out.concat(data.Items);
        }
    });

    //get posts from username
    params = {
        TableName: 'posts',
        IndexName: 'poster-postid-index',
        KeyConditionExpression: 'poster = :username',
        ExpressionAttributeValues: {
            ':username': { S: username },
        },
    };

    db.query(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            out = out.concat(data.Items);
            callback(null, out);
        }
    });
};

var myDB_get_posts_homepage = function (username, callback) {
    console.log('Beginning post table query.');

    var out = [];

    //get usernames friends
    let params = {
        TableName: 'friends',
        KeyConditionExpression: '#user = :username',
        ExpressionAttributeNames: {
            '#user': 'user',
        },
        ExpressionAttributeValues: {
            ':username': { S: username },
        },
    };
    console.log('Beginning friends search');
    db.query(params, function (err, data) {
        if (err) {
            console.log('failed friends search');
            callback(err, null);
        } else {
            for (var i = 0; i < data.Items.length; i++) {
                console.log(data.Items[i].friend.S);
                params = {
                    TableName: 'posts',
                    IndexName: 'postee-poster-index',
                    KeyConditionExpression: 'postee = :friend',
                    FilterExpression: 'postee <> :username',
                    ExpressionAttributeValues: {
                        ':friend': { S: data.Items[i].friend.S },
                        ':username': { S: username },
                    },
                };
                db.query(params, function (err, data2) {
                    if (err) {
                        callback(err, null);
                    } else {
                        out = out.concat(data2.Items);
                    }
                });
            }
            //get posts to username
            params = {
                TableName: 'posts',
                KeyConditionExpression: 'postee = :username',
                FilterExpression: 'postee <> :username',
                ExpressionAttributeValues: {
                    ':username': { S: username },
                },
            };

            db.query(params, function (err, data) {
                if (err) {
                    callback(err, null);
                } else {
                    out = out.concat(data.Items);
                    //get posts from username
                    params = {
                        TableName: 'posts',
                        IndexName: 'poster-postid-index',
                        KeyConditionExpression: 'poster = :username',
                        ExpressionAttributeValues: {
                            ':username': { S: username },
                        },
                    };

                    db.query(params, function (err, data) {
                        if (err) {
                            callback(err, null);
                        } else {
                            console.log(out);
                            out = out.concat(data.Items);
                            console.log(out);
                            callback(null, out);
                        }
                    });
                }
            });
        }
    });
};

var myDB_post = function (title, content, poster, postee, callback) {
    let params = {
        TableName: 'friends',
        KeyConditionExpression: '#user = :username',
        ExpressionAttributeNames: {
            '#user': 'user',
        },
        ExpressionAttributeValues: {
            ':username': { S: username },
        },
    };
    db.query(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            let current = new Date();
            let cDate =
                current.getFullYear() +
                '-' +
                (current.getMonth() + 1) +
                '-' +
                current.getDate();
            let cTime =
                current.getHours() +
                ':' +
                current.getMinutes() +
                ':' +
                current.getSeconds();
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
                        S: dateTime,
                    },
                    postid: {
                        S: poster + cTime,
                    },
                },
            };
            db.putItem(params, function (err, data) {
                if (err) {
                    console.log(err);
                }
                callback(err, data);
            });
        }
    });
};

var get_users_chats = function (user, callback) {
    //console.log('looking for chats containing user');
    //console.log(user);
    //get chats containing user
    params = {
        TableName: 'chats',
        IndexName: 'sortkey-chatID-index',
        KeyConditionExpression: 'sortkey = :user',
        ExpressionAttributeValues: {
            ':user': { S: 'member_' + user },
        },
    };

    db.query(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            //console.log('found chats');
            //console.log(data);
            callback(err, data.Items);
        }
    });
};

/**
Searches the Chats table for specific chat and checks if the given user is in the chat.
If the user is in the chat, data contains TRUE. Otherwise, data contains FALSE */
var in_chat = function (user, chatid, callback) {
    //console.log(
    //'checking if user ' + user + ' has access to chat with id ' + chatid
    //);
    params = {
        TableName: 'chats',
        KeyConditionExpression: 'chatID = :chatid and sortkey = :sortkey',
        ExpressionAttributeValues: {
            ':chatid': { S: chatid },
            ':sortkey': { S: 'member_' + user },
        },
    };

    db.query(params, function (err, data) {
        if (err) {
            console.log(err);
        }
        callback(err, data);
    });
};

var get_chat = function (chatid, callback) {
    //console.log('retreiving message from chat with id ' + chatid);
    params = {
        TableName: 'chats',
        KeyConditionExpression:
            'chatID = :chatid and begins_with(sortkey, :msg)',
        ExpressionAttributeValues: {
            ':chatid': { S: chatid },
            ':msg': { S: 'message' },
        },
    };

    db.query(params, function (err, data) {
        if (err) {
            console.log(err);
        }
        console.log('found ' + data.Items.length + ' messages');
        //console.log(data);
        callback(err, data);
    });
};

/**
Puts an item into the chats table corresponding to <message> being sent by <user> in chat <chatid>
The sortkey for a message in the chats table is of the form message_<timestamp>_<messageid>.
We will produce the timestamp using javascripts native methods, and produce a unique message id
using nodes native crypto module. The message id doesnt have to be super long as itll only be needed
in the rare case that two messages are sent by different users at the exact same time.
 */
var send_message = function (chatid, message, user, callback) {
    //console.log('adding new message to chat ' + chatid);
    //get the timestamp
    var today = new Date();
    //we must add 0s in front of months and days that are single digits so order is maintained
    //in the database properly (13 would be placed ahead of 6 otherwise for example)
    var month = today.getMonth() + 1;
    if (month < 10) {
        month = '0' + month;
    }
    var day = today.getDate();
    if (day < 10) {
        day = '0' + day;
    }

    //putting all the time data together to create the timestamp and sortkey
    var timestamp =
        today.getFullYear() +
        '-' +
        month +
        '-' +
        day +
        '-' +
        today.getHours() +
        ':' +
        today.getMinutes() +
        ':' +
        today.getSeconds();
    var messageid = crypto.randomBytes(16).toString('hex').slice(24);
    var params = {
        Item: {
            chatID: {
                S: chatid,
            },
            sortkey: {
                S: 'message_' + timestamp + '_' + messageid,
            },
            createdAt: {
                S: timestamp,
            },
            username: {
                S: user,
            },
            message: {
                S: message,
            },
        },
        TableName: 'chats',
        ReturnValues: 'NONE',
    };

    //put the given item into the table
    db.putItem(params, function (err, data) {
        if (err) {
            callback(err);
        } else {
            callback(null, 'Success');
        }
    });
};

var myDB_search_partial_update = function (
    partialUsername,
    username,
    callback
) {
    console.log('Posting partial: ' + partialUsername);
    let params = {
        TableName: 'search',
        Key: {
            username: { S: partialUsername },
        },
    };

    db.getItem(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            if (data.Item) {
                let results = data.Item.results.L;
                results.push({ S: username });
                results.sort((a, b) => a.S.localeCompare(b.S));
                if (results.length > 10) results.pop();
                params = {
                    TableName: 'search',
                    Key: {
                        username: {
                            S: partialUsername,
                        },
                    },
                    UpdateExpression: 'set results = :results',
                    ExpressionAttributeValues: {
                        ':results': { L: results },
                    },
                    ReturnValues: 'UPDATED_NEW',
                };
                db.updateItem(params, function (err, data) {
                    if (err) {
                        console.log(err);
                    }
                    callback(err, data);
                });
            } else if (!data.Item) {
                params = {
                    TableName: 'search',
                    Item: {
                        username: {
                            S: partialUsername,
                        },
                        results: {
                            L: [{ S: username }],
                        },
                    },
                };
                db.putItem(params, function (err, data) {
                    if (err) {
                        console.log(err);
                    }
                    callback(err, data);
                });
            }
        }
    });
};

var myDB_search_partial = function (searchTerm, callback) {
    let params = {
        TableName: 'search',
        Key: {
            username: { S: searchTerm },
        },
    };
    db.getItem(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });
};

var myDB_get_friends = function (username, callback) {
    let params = {
        TableName: 'friends',
        KeyConditionExpression: '#user = :username',
        ExpressionAttributeNames: {
            '#user': 'user',
        },
        ExpressionAttributeValues: {
            ':username': { S: username },
        },
    };
    db.query(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });
};

var myDB_add_friend = function (username, friend, callback) {
    console.log('Adding friend between: ' + username + ' and ' + friend);
    let params = {
        TableName: 'friends',
        Item: {
            user: { S: username },
            friend: { S: friend },
        },
    };
    db.putItem(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });

    params = {
        TableName: 'friends',
        Item: {
            user: { S: friend },
            friend: { S: username },
        },
    };
    db.putItem(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });
};

var myDB_remove_friend = function (username, friend, callback) {
    let params = {
        TableName: 'friends',
        Key: {
            user: { S: username },
            friend: { S: friend },
        },
    };
    db.deleteItem(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });

    params = {
        TableName: 'friends',
        Key: {
            user: { S: friend },
            friend: { S: username },
        },
    };
    db.deleteItem(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });
};

var myDB_check_friend_status = function (username, otherperson, callback) {
    let params = {
        TableName: 'friends',
        KeyConditionExpression: '#u = :username and friend = :friend',
        ExpressionAttributeNames: {
            '#u': 'user',
        },
        ExpressionAttributeValues: {
            ':username': { S: username },
            ':friend': { S: otherperson },
        },
    };
    db.query(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });
};

var update_last_online = function (username) {
    let d = new Date();
    let params = {
        TableName: 'users',
        Key: {
            username: {
                S: username,
            },
        },
        UpdateExpression: 'set last_online = :last_online',
        ExpressionAttributeValues: {
            ':last_online': { N: (d.getTime() / 1000).toString() },
        },
        ReturnValues: 'UPDATED_NEW',
    };
    db.updateItem(params, function (err, data) {});
};

var check_last_online = function (username, callback) {
    let params = {
        TableName: 'users',
        Key: {
            username: {
                S: username,
            },
        },
        AttributesToGet: ['last_online'],
    };
    db.getItem(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });
};

var create_chat = function (title, members, callback) {
    //going to use similar methodology to random "unique" ids as was used for message ids
    //get the timestamp
    var today = new Date();
    //we must add 0s in front of months and days that are single digits so order is maintained
    //in the database properly (13 would be placed ahead of 6 otherwise for example)
    var month = today.getMonth() + 1;
    if (month < 10) {
        month = '0' + month;
    }
    var day = today.getDate();
    if (day < 10) {
        day = '0' + day;
    }

    //putting all the time data together to create the timestamp and sortkey
    var timestamp =
        today.getFullYear() +
        '-' +
        month +
        '-' +
        day +
        '-' +
        today.getHours() +
        ':' +
        today.getMinutes() +
        ':' +
        today.getSeconds();

    //ugly timestamp with only alphanumerics so the chatid can go in a url
    var timestampUGLY =
        today.getFullYear() +
        month +
        day +
        today.getHours() +
        today.getMinutes() +
        today.getSeconds();
    //generates a randomstring of 16 characters
    var uid = crypto.randomBytes(16).toString('hex').slice(16);
    var chatid = uid + timestampUGLY;

    var params = {
        Item: {
            chatID: {
                S: chatid,
            },
            sortkey: {
                S: 'config',
            },
            createdAt: {
                S: timestamp,
            },
            chatName: {
                S: title,
            },
        },
        TableName: 'chats',
        ReturnValues: 'NONE',
    };
    db.putItem(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            console.log('made the config row');
            //now we need to add all members to the chat
            for (i in members) {
                console.log('adding member');
                let params = {
                    Item: {
                        chatID: {
                            S: chatid,
                        },
                        sortkey: {
                            S: 'member_' + members[i],
                        },
                        createdAt: {
                            S: timestamp,
                        },
                    },
                    TableName: 'chats',
                    ReturnValues: 'NONE',
                };
                db.putItem(params, function (err, data) {
                    if (err) {
                        console.log(err);
                    }
                });
            }
            callback(err, data);
        }
    });
};

var post_a_post = function (inputData, callback) {
    let d = new Date();
    let params = {
        TableName: 'posts',
        Item: {
            poster: {
                S: inputData.username,
            },
            title: {
                S: inputData.title,
            },
            content: {
                S: inputData.content,
            },
            time: {
                N: d.getTime().toString(),
            },
        },
    };
    if (inputData.wall) {
        params.Item.wall = {
            S: inputData.wall,
        };
    }
    db.putItem(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });
};

var get_posts_by_user = function (username, callback) {
    let params = {
        TableName: 'posts',
        KeyConditionExpression: 'poster = :username',
        ExpressionAttributeValues: {
            ':username': {
                S: username,
            },
        },
    };
    db.query(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });
};

var get_posts_to_wall = function (wall, callback) {
    let params = {
        TableName: 'posts',
        IndexName: 'wall-index',
        KeyConditionExpression: 'wall = :wall',
        ExpressionAttributeValues: {
            ':wall': {
                S: wall,
            },
        },
    };
    db.query(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });
};

var delete_post = function (data, callback) {
    let params = {
        TableName: 'posts',
        Key: {
            poster: {
                S: data.username,
            },
            time: {
                N: data.time,
            },
        },
    };
    db.deleteItem(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, data);
        }
    });
};
var get_title = function (chatid, callback) {
    params = {
        TableName: 'chats',
        KeyConditionExpression: 'chatID = :chatid and sortkey = :msg',
        ExpressionAttributeValues: {
            ':chatid': { S: chatid },
            ':msg': { S: 'config' },
        },
    };

    db.query(params, function (err, data) {
        if (err) {
            console.log(err);
        }
        //console.log(data);
        if (data) {
            callback(err, data.Items[0]);
        } else {
            callback(err, data);
        }
    });
};
var get_friends_visualizer = function (username, nodeid, callback) {
    myDB_data_user_profile(username, function (err, data) {
        let userAffiliation = data.Item.affiliation.S;
        myDB_get_friends(nodeid, function (err, data) {
            let promises = [];
            for (friend of data.Items) {
                promises.push(
                    new Promise((resolve, reject) => {
                        myDB_data_user_profile(
                            friend.friend.S,
                            function (err, data) {
                                if (err) reject(err);
                                resolve(data.Item);
                            }
                        );
                    })
                );
            }

            Promise.all(promises).then((data) => {
                let ret = [];
                for (friend of data) {
                    if (!friend.affiliation.S.localeCompare(userAffiliation)) {
                        ret.push(friend);
                    }
                }
                callback(err, ret);
            });
        });
    });
};
/*var myDB_search_all = function (searchTerm, callback) {
    let params = {
        TableName: 'users',
    };
    db.scan(params, function (err, data) {
        if (err) {
            callback(err, null);
        } else {
            let filtered = data.Items.filter((item) => {
                return item.username.S.startsWith(searchTerm);
            });
            callback(err, filtered);
        }
    });
};*/

var database = {
    login_check: myDB_login_check,
    new_acc_check: myDB_new_acc_check,
    get_user_profile_data: myDB_data_user_profile,
    update_user_profile: update_user_profile,
    get_posts_w: myDB_get_posts_wall,
    get_posts_hp: myDB_get_posts_homepage,
    new_post: myDB_post,
    get_users_chats: get_users_chats,
    get_partial_users: myDB_search_partial,
    post_partial_search: myDB_search_partial_update,
    in_chat: in_chat,
    get_chat: get_chat,
    send_message: send_message,
    get_friends: myDB_get_friends,
    add_friend: myDB_add_friend,
    remove_friend: myDB_remove_friend,
    check_friend_status: myDB_check_friend_status,
    update_last_online: update_last_online,
    check_last_online: check_last_online,
    get_friends_visualizer: get_friends_visualizer,
    check_valid_user: check_valid_user,
    post_a_post: post_a_post,
    get_posts_by_user: get_posts_by_user,
    delete_post: delete_post,
    get_posts_to_wall: get_posts_to_wall,
    create_chat: create_chat,
    get_title: get_title,
};

module.exports = database;
