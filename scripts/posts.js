async function loadData() {
    return $.getJSON('/posts');
}

async function fetchAllPosts() {
	console.log("Testing console printing");
    let data = await loadData();
	let table = document.getElementById('test');
	for(let i in data) {
		var row = table.insertRow(0);
		var cell = row.insertCell(0);

		cell.innerHTML = data[i]["postId"].S;
		
	}
	//document.getElementById('test').innerText = data[0]["sortkey"].S;
}