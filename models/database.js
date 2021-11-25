var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-1' });
var db = new AWS.DynamoDB();
const bcrypt = require('bcrypt');

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
            '#un': 'name',
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
                            console.log(res);
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
var myDB_new_acc_check = function (
    inputUname,
    inputPword,
    inputFname,
    callback
) {
    console.log(
        'New Acc Check: Querying for username: ' +
            inputUname +
            ', password: ' +
            inputPword +
            ', full name: ' +
            inputFname
    );

    let params = {
        TableName: 'users',
        KeyConditionExpression: '#un = :uname',
        ExpressionAttributeNames: {
            '#un': 'name',
        },
        ExpressionAttributeValues: {
            ':uname': {
                S: inputUname,
            },
        },
    };
    const saltRounds = 10;
    bcrypt
        .hash(inputPword, saltRounds)
        .then((hash) => {
            console.log(`Hash: ${hash}`);
            db.query(params, function (err, data) {
                if (
                    inputUname.length == 0 ||
                    inputPword.length == 0 ||
                    inputFname.length == 0
                ) {
                    callback('Complete all inputs!', null);
                } else {
                    if (err) {
                        console.log('putItem');
                        callback(err, null);
                    } else {
                        if (data.Items.length > 0) {
                            callback('User already existed!', null);
                        } else {
                            let params = {
                                TableName: 'users',
                                Item: {
                                    name: {
                                        S: inputUname,
                                    },
                                    password: {
                                        S: hash,
                                    },
                                    fullname: {
                                        S: inputFname,
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
                                    console.log(
                                        'Added item:',
                                        JSON.stringify(data, null, 2)
                                    );
                                    callback(null, 'New account created.');
                                }
                            });
                        }
                    }
                }
            });
        })
        .catch((err) => console.error(err.message));
};

async function sha256(message) {
    // encode as UTF-8
    const msgBuffer = new TextEncoder().encode(message);

    // hash the message
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);

    // convert ArrayBuffer to Array
    const hashArray = Array.from(new Uint8Array(hashBuffer));

    // convert bytes to hex string
    const hashHex = hashArray
        .map((b) => b.toString(16).padStart(2, '0'))
        .join('');
    return hashHex;
}
var database = {
    login_check: myDB_login_check,
    new_acc_check: myDB_new_acc_check,
};

module.exports = database;
