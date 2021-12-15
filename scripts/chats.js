async function loadData() {
    return $.getJSON('/chats');
}

async function getTitle(chatnum) {
	return $.getJSON('/data/chat/titles/'+chatnum);
}

async function fetchAllChats() {
	console.log("Testing console printing");
    let data = await loadData();
	let table = document.getElementById('test');
	let group_chats = document.getElementById('group-chats');
	let promises = [];
	for(let i in data) {
		let chatnum = data[i]["chatID"].S;
		promises.push(getTitle(chatnum));

	}
	Promise.all(promises).then(results => {
		results.forEach(result => {
			let id = result["chatID"].S;
			let title = result["chatName"].S;
			
			let chat_info_card = document.createElement("a");
				chat_info_card.setAttribute("href", "/chat?chatid="+id);
				chat_info_card.setAttribute("class", "list-group-item list-group-item-action");
				chat_info_card.setAttribute("aria-current", "true");
				
			let chat_container = document.createElement("div");
				chat_container.setAttribute("class", "d-flex w-100 justify-content-between");
				
			let chat_title = document.createElement("h5");
				chat_title.setAttribute("class", "mb-1");
				chat_title.setAttribute("style", "font-weight: bold");
				chat_title.innerHTML = title
				
			
			let recent_message_timestamp = document.createElement("small");
				recent_message_timestamp.setAttribute("class", "text-muted");
				if(result["lastTime"]) {
					recent_message_timestamp.innerHTML = result["lastTime"].S;
				} else {
					recent_message_timestamp.innerHTML = "";
				}
				
			let recent_message = document.createElement("p");
				recent_message.setAttribute("class", "mb-1");
				if(result["message"]) {
					recent_message.innerHTML = result["message"].S;
				} else {
					recent_message.innerHTML = "No message history";
				}
				
			let recent_sender = document.createElement("small");
				recent_sender.setAttribute("class", "text-muted");
				if(result["username"]) {
					recent_sender.innerHTML = result["username"].S;
				} else {
					recent_sender.innerHTML = "";
				}
				
			chat_container.append(chat_title);
			chat_container.append(recent_message_timestamp);
			
			chat_info_card.append(chat_container);
			chat_info_card.append(recent_message);
			chat_info_card.append(recent_sender);
			
			group_chats.append(chat_info_card);
		});
	});
	/*
	<a href="/chat?chatid=(chatid)" class="list-group-item list-group-item-action active" aria-current="true">
    <div class="d-flex w-100 justify-content-between">
      <h5 class="mb-1">Chat Title</h5>
      <small>Time that most recent message was sent</small>
    </div>
    <p class="mb-1">
      Most recent chat message
    </p>
    <small>User who sent most recent message</small>
  </a>
	*/
}