function loadData() {
    $.getJSON('/data/userprofile', function (result) {
        let data = result.Item;
        console.log(data);
        document.getElementById('firstname').innerText = data.firstname.S;
        document.getElementById('lastname').innerText = data.lastname.S;
    });
}
