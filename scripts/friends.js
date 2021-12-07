function initFriends() {
    loadFriends();
    setInterval(() => loadFriends(), 5000);
}

function loadFriends() {
    let d = new Date();
    let currTime = parseInt(d.getTime() / 1000);
    $.post('/getfriends', function (results) {
        let promises = [];
        if (results.Items) {
            for (result of results.Items) {
                promises.push(
                    $.post(`/lastonline/${result.friend.S}`).then(
                        (queryResults) => {
                            if (queryResults.Item.last_online) {
                                let diff = parseInt(
                                    (currTime -
                                        parseInt(
                                            queryResults.Item.last_online.N
                                        )) /
                                        60
                                );
                                if (diff >= 5) {
                                    // Users with actions within 5 seconds are considered online
                                    return `<a href="/user/${queryResults.user}" style="display: block">${queryResults.user} (last online ${diff} minutes ago)</a>`;
                                } else {
                                    return `<a href="/user/${queryResults.user}" style="display: block">${queryResults.user} <span style="color: green">(online)</span></a>`;
                                }
                            } else {
                                return `<a href="/user/${queryResults.user}" style="display: block">${queryResults.user} (no last online data available)</a>`;
                            }
                        }
                    )
                );
            }
        } else {
            content += '<p>None</p>';
        }
        Promise.all(promises).then((lines) => {
            let content = '';
            for (line of lines) {
                content += line;
            }
            document.getElementById('friends-display').innerHTML = content;
        });
    });
}
