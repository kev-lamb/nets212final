async function loadData(chatid) {
    return $.getJSON('/data/chat?chatid='+chatid);
}

async function fetchMessages() {
	console.log("Testing console printing");
    let data = await loadData();
	let table = document.getElementById('test');
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
		console.log("found a message");
		var content = table.insertRow();
		var contentcell = content.insertCell();
		var poster = table.insertRow();
		var postercell = poster.insertCell();
		
		contentcell.innerHTML = messages[i]["message"].S;
		poster.innerHTML = messages[i]["username"].S;
	}
};

async function send_message(chatid) {
	console.log("sending the following message to chat with id " +chatid+":");
	$.post('/sendmessage', {chatid: chatid, message: $( "#new_message" ).val()}, function(data) {
		console.log(data);
	});
}
