async function loadData() {
    return $.getJSON('/chat');
}

async function fetchAllChats() {
    let data = (await loadData());
	document.getElementByID('test').innerText = data.S;
}