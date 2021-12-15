var socket = io();
var chatid = null;
var username = null;
var chat = null;
var messageInput = null;
var chatBox = null;
var num_messages = 0;

/*
Called when you first load the page. Initializes importsant document elements as variables
to be referenced in many other functions, and sets up code so the chat can update without reloading the page
*/
$(document).ready(function() {
	chatid = document.getElementById('chatid').innerHTML;
	chat = document.getElementById('message_form');
	messageInput = document.getElementById('new_message');
	chatBox = document.getElementById('chat-box');
	username = document.getElementById('username').innerHTML;
	console.log(chat);
	chat.addEventListener('submit', function(event) {
		event.preventDefault();
		send_message(chatid, messageInput.value);
	});
	
});

/*
Runs when the server broadcasts a chat message to all users with the chat open
Adds the new message to the chat if the chatid matches the current opened chat
*/
socket.on('chat-message', function(data) {
	if(chatid == data.chatid) {
		//add the message to the bottom of the chat log
		appendMessage(data.message, data.user);
		
		//autoscroll to the bottom of the chat if not there already
		if (chatBox.scrollHeight - chatBox.offsetHeight - chatBox.scrollTop > 1) {
		chatBox.scrollTop = chatBox.scrollHeight;
		}
	}
});

function appendMessage(message, username) {
	/*
	let table = document.getElementById('display');
	var content = table.insertRow();
	var contentcell = content.insertCell();
	var poster = table.insertRow();
	var postercell = poster.insertCell();
	contentcell.innerText = message;
	postercell.innerText = username;
	*/

	console.log(num_messages);
	
	let messagecontainer = document.createElement("div");
	messagecontainer.setAttribute("id", "message-container-"+(num_messages % 2));
		
	let messageusername = document.createElement("div");
	messageusername.setAttribute("id", "message-username");
	messageusername.innerHTML = username;
	messagecontainer.append(messageusername);
		
	let messagecontent = document.createElement("div");
	messagecontent.setAttribute("id", "message-content");
	messagecontent.innerHTML = message;
	messagecontainer.append(messagecontent);
		
	chatBox.append(messagecontainer);
	num_messages++;
	
};

async function loadData(chatid) {
    return $.getJSON('/data/chat?chatid='+chatid);
}

async function getTitle(chatnum) {
	return $.getJSON('/data/chat/titles/'+chatnum);
}

async function getChatters(chatid) {
	return $.getJSON('/data/users/'+chatid);
}


async function fetchMessages() {
    let data = await loadData();
	let table = document.getElementById('display');
	for(let i in data) {
		var row = table.insertRow(0);
		var cell = row.insertCell(0);

		cell.innerHTML = data[i]["chatID"].S;
		
	}
	//document.getElementById('test').innerText = data[0]["sortkey"].S;
};

async function populate_chat_onLoad(chatid){
	
	console.log("putting all messages in the chat");
	let table = document.getElementById('display');
	console.log(chatid);
	let messages = (await loadData(chatid)).Items;
	for (let i = 0; i < messages.length; i++) {
		/*console.log("found a message");
		var content = table.insertRow();
		var contentcell = content.insertCell();
		var poster = table.insertRow();
		var postercell = poster.insertCell();
		
		contentcell.innerHTML = messages[i]["message"].S;
		poster.innerHTML = messages[i]["username"].S;
		*/
		let messagecontainer = document.createElement("div");
		messagecontainer.setAttribute("id", "message-container-"+(i%2));
		
		let messageusername = document.createElement("div");
		messageusername.setAttribute("id", "message-username");
		messageusername.innerHTML = messages[i]["username"].S;
		messagecontainer.append(messageusername);
		
		let messagecontent = document.createElement("div");
		messagecontent.setAttribute("id", "message-content");
		messagecontent.innerHTML = messages[i]["message"].S;
		messagecontainer.append(messagecontent);
		
		chatBox.append(messagecontainer);
		num_messages++;
		
		
	}
	
	
	//once chat has been populated, add all chat members as options in the chat member dropdown
	let members = (await getChatters(chatid)).Items;
	console.log(members);
	let dropdown_menu = document.getElementById("members-dropdown");
	for(i in members) {
		//remove the member_ prefix to the sortkey
		let curr_name = members[i]["sortkey"].S.slice(7);
		console.log(curr_name);
		
		//compare name to user because we dont want their name to appear in the list
		if(!(curr_name == username) ) {
			//add name to the dropdown menu with a link back to their user page
			console.log("trying to add the thing to the dropdown");
			let tomato = document.createElement("a");
			tomato.setAttribute("href", "/user/"+curr_name);
			tomato.setAttribute("class", "dropdown-item");
			tomato.innerHTML = curr_name;
			dropdown_menu.append(tomato);
			//<a href="/user/:username" class="dropdown-item">:username</a>
		}
	}
	
	get_online_friends();
	
};

/*
Adds new message to the clients chat box, then broadcasts thew new message to the server
and sends message to the database
*/
async function send_message(chatid, message) {
	//console.log("sending the following message to chat with id " +chatid+":");
	//console.log($("#new_message").val());
	appendMessage(message, username);
	
	//autoscroll to the bottom of the chat if not there already
	if (chatBox.scrollHeight - chatBox.offsetHeight - chatBox.scrollTop > 1) {
		chatBox.scrollTop = chatBox.scrollHeight;
	}
	
	socket.emit('send-chat-message', {message: message, username: username, chatid: chatid});
	$.post('/sendmessage', {chatid: chatid, message: message}, function(data) {
		console.log(data);
	});
	messageInput.value = '';
}

//populate a dropdown menu with all online friends when the dropdown menu is clicked on
async function get_online_friends() {
	
	
	let d = new Date();
    let currTime = parseInt(d.getTime() / 1000);
    $.post('/getfriends', function (results) {
        let promises = [];
        if (results.Items) {
            for (result of results.Items) {
                promises.push(
                    $.post(`/lastonline/${result.friend.S}`).then(
                        (queryResults) => {
							console.log(queryResults);
                            if (queryResults.Item.last_online) {
                                let diff = parseInt(
                                    (currTime -
                                        parseInt(
                                            queryResults.Item.last_online.N
                                        )) /
                                        60
                                );
								console.log(diff);
                                if (diff < 5) {
                                    // Users with actions within 5 minutes are considered online
									//each of these users should be added to the dropdown menu
									let tomato = document.createElement("a");
									tomato.setAttribute("href", "#");
									tomato.setAttribute("class", "dropdown-item");
									tomato.setAttribute("style", "color: green");
									tomato.setAttribute("onclick", "addUser("+"'"+queryResults.user + "'); return false;")
									tomato.innerHTML = queryResults.user;
									console.log("returning a tomato");
									console.log(tomato);
									return tomato;

                                } else {
									return null;
}
                            }
                        }
                    )
                );
            }
        } else {
            content += '<p>None</p>';
        }
        Promise.all(promises).then((online_friends) => {
            let available_friends = document.getElementById("online-friends-dropdown");
            for (friend of online_friends) {
				if(friend) {
                	available_friends.append(friend);
				}
            }
        });
    });


};


/*
Emits message to the server inviting a user with username to join chat with chatid. Server will then broadcast the message to the user
*/
async function addUser(user_to_invite) {
	console.log(document.getElementById('title'));
	socket.emit('invite-user-to-chat', {
		user_invited: user_to_invite,
		chatid: chatid,
		user_inviting: username,
		chat_title: document.getElementById("title").innerText
		});
};



