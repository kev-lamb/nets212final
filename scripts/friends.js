function initFriends() {
    loadFriends();
    setInterval(() => loadFriends(), 5000);
}

function loadFriends() {
    $.post('/getfriends', function (results) {
        let content = '';
        if (results.Items) {
            for (result of results.Items) {
                content += `<a href="/user/${result.friend.S}" style="display: block">${result.friend.S}</a>`;
            }
        } else {
            content += '<p>None</p>';
        }
        document.getElementById('friends-display').innerHTML = content;
    });
}
