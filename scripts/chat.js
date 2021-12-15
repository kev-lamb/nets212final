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

//code for handling chat autoscrolling to most recent messages
function isFeedAtBottom() {
    return (this.chatBox.offsetHeight+this.chatBox.scrollTop)===this.chatBox.scrollHeight;
};

function scrollFeedToBottom() {
    this.chatBox.scrollTop = this.chatBox.scrollHeight;
};

