async function loadData() {
    return $.getJSON('/yourfriends');
}

async function populate_form_with_friends() {
    //let data = (await loadData());
	//console.log(data);
	let form = document.getElementById('chat-form');
	var br = document.createElement("br");
	/*for(let i in data) {
		let friend = data[i]["friend"].S;
		let elm = document.createElement("input");
    	elm.setAttribute("type", "checkbox");
    	elm.setAttribute("name", friend);
    	elm.setAttribute("id", friend);
		form.appendChild(elm);
		form.appendChild(br.cloneNode());
		
	}*/
	try {
		let friends = (await loadData()).Items;
		for (let i = 0; i < friends.length; i++) {
			let elm = document.createElement("input");
    		elm.setAttribute("type", "checkbox");
    		elm.setAttribute("name", friends[i]["friend"].S);
    		elm.setAttribute("id", friends[i]["friend"].S);
			
			let label = document.createElement("Label");
			label.setAttribute("for", friends[i]["friend"].S);
			label.innerHTML = friends[i]["friend"].S;
			form.appendChild(label);
			
			form.appendChild(elm);
			form.appendChild(br.cloneNode());
		}
	} catch(e) {
		console.log(e);
	}
	
	//document.getElementById('test').innerText = data[0]["sortkey"].S;
}