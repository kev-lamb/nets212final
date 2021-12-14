async function loadData() {
    return $.getJSON('/chats');
}

async function fetchAllChats() {
	console.log("Testing console printing");
    let data = await loadData();
	let table = document.getElementById('test');
	for(let i in data) {
		var row = table.insertRow(0);
		var cell = row.insertCell(0);
		let chatnum = data[i]["chatID"].S;
		cell.innerHTML = "<a href=" + "'" + "/chat?chatid=" + chatnum + "'>"+chatnum+"</a>";
		
	}
	//document.getElementById('test').innerText = data[0]["sortkey"].S;
}