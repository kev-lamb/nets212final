function loadPage() {
    initPost();
}

function initPost() {
    loadPost();
    setInterval(() => loadPost(), 5000);
}
function postNewPost() {
    if (!FormValidation('createPost')) {
        return false;
    }
    let title = document.getElementById('title').value;
    let content = document.getElementById('content').value;
    console.log(title);
    console.log(content);
    let data = {
        username: person,
        title: title,
        content: content,
    };
    $.post('/makeapost', data, function () {
        console.log('finish post');
        loadPost();
    });
    document.getElementById('title').value = '';
    document.getElementById('content').value = '';
    return false;
}
function loadPost() {
    let data = {
        username: person,
    };
    let content = '';
    let promisesBigQuery = [];
    promisesBigQuery.push(
        $.get('/getposts', data, function (result) {
            return result.Items.reverse();
        }).then((ret) => ret.Items)
    );
    let friends = new Set();
    promisesBigQuery.push(
        $.post('/getfriends')
            .then((results) => {
                let promises = [];
                if (results.Items) {
                    for (result of results.Items) {
                        let data = {
                            username: result.friend.S,
                        };
                        friends.add(result.friend.S);
                        promises.push(
                            $.get('/getposts', data).then((queryResults) => {
                                return queryResults;
                            })
                        );
                    }
                } else {
                    content += '<p>None</p>';
                }
                return promises;
            })
            .then((promises) => Promise.all(promises))
    );
    Promise.all(promisesBigQuery).then((results) => {
        let arr = results[0];
        for (result of results[1]) {
            for (item of result.Items) {
                if (
                    item.poster.S == person ||
                    !item.wall ||
                    item.wall.S == person ||
                    friends.has(item.wall.S)
                )
                    arr.push(item);
            }
        }
        arr.sort((a, b) => a.time.N - b.time.N);
        for (item of arr.reverse()) {
            content += createPost(item);
        }
        document.getElementById('posts').innerHTML = content;
    });
}
/*
.then((lines) => {
                lines.then((res) => {
                    console.log(res);
                });
                return lines.then((res) => res);
            })
            .then((res) => {
                console.log(res);
            })*/

function deletePost(username, time) {
    console.log('deleting post');
    let data = {
        username: username,
        time: time,
    };
    $.post('/deletepost', data, function () {
        loadPost();
    });
}

function test() {
    console.log('inTest');
}
