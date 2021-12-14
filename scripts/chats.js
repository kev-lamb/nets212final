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
	let promises = [];
	for(let i in data) {
		let chatnum = data[i]["chatID"].S;
		promises.push(getTitle(chatnum));

	}
	Promise.all(promises).then(results => {
		results.forEach(result => {
			let id = result["chatID"].S;
			let title = result["chatName"].S;
			let row = table.insertRow(0);
			let cell = row.insertCell(0);
			cell.innerHTML = "<a href=" + "'" + "/chat?chatid=" + id + "'>"+title+"</a>";
		});
	});
	//document.getElementById('test').innerText = data[0]["sortkey"].S;
}