var usrname = document.getElementById('otherperson').innerText;
var isFriends = false;
let myInterval = '';
function loadPage() {
    if (!mypage) {
        document.getElementById('createPost').style.display = 'none';
        initButton();
    } else {
        document.getElementById('postOnSomeoneElsesWall').style.display =
            'none';
        document.getElementById('friendButtonDiv').style.display = 'none';
    }
    if (isFriends || mypage) {
        initPost();
    }
}
function initButton() {
    setButton();
    //setInterval(() => setButton(), 20000);
}

function initPost() {
    loadPost();
    //myInterval = setInterval(() => loadPost(), 20000);
}

function checkForPosts() {
    if (isFriends || mypage) {
        initPost();
    } else {
        clearInterval(myInterval);
        document.getElementById('posts').innerHTML =
            'Become Friends to See Posts';
    }
}

function setButton() {
    let data = {
        user: usrname,
    };
    $.post('/checkfriendstatus', data, function (results) {
        let element = document.getElementById('friendbutton');
        element.classList.remove('btn-success');
        element.classList.remove('btn-danger');
        if (results.Items.length == 0) {
            isFriends = false;
            element.innerText = 'Add Friend';
            element.classList.add('btn-success');
        } else {
            element.innerText = 'Remove Friend';
            element.classList.add('btn-danger');
            isFriends = true;
        }
        checkForPosts();
    });
}
function changeFriendStatus() {
    let data = {
        user: document.getElementById('otherperson').innerText,
    };
    let route = '';
    if (document.getElementById('friendbutton').innerText == 'Add Friend') {
        route = '/addfriend';
        let title = me + ' sent a friend request to  ' + person;
        let newFriendData = {
            username: me,
            title: title,
            wall: document.getElementById('otherperson').innerText,
        };
        $.post('/makeapost', newFriendData, function () {
            loadPost();
        }).then(() => {
            let title = person + ' accepted ' + me + "'s friend request";
            let newFriendData = {
                username: document.getElementById('otherperson').innerText,
                title: title,
                wall: me,
                request: 'PennBook Bot',
            };
            $.post('/makeapost', newFriendData, function () {});
        });
    } else if (
        document.getElementById('friendbutton').innerText == 'Remove Friend'
    ) {
        route = '/removefriend';
    }
    $.post(route, data, function (results) {
        setButton();
    });
}
function postNewPost(id) {
    if (!FormValidation(id)) {
        return false;
    }
    let title = document.getElementById('title').value;
    let content = document.getElementById('content').value;
    if (id == 'createPostElse') {
        title = document.getElementById('titleElse').value;
        content = document.getElementById('contentElse').value;
    }
    let data = {
        title: title,
        content: content,
        wall: person,
    };
    $.post('/makeapost', data, function () {
        console.log('finish post');
        loadPost();
    });
    document.getElementById('title').value = '';
    document.getElementById('content').value = '';
    if (id == 'createPostElse') {
        document.getElementById('titleElse').value = '';
        document.getElementById('contentElse').value = '';
    }
    return false;
}

function loadPost() {
    let active = document.activeElement.id;
    console.log('load posts');
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
    data = {
        wall: person,
    };
    promisesBigQuery.push(
        $.get('/getwallposts', data).then((ret) => ret.Items)
    );
    Promise.all(promisesBigQuery)
        .then((results) => {
            let arr = [...results[0], ...results[1]];
            arr = arr.filter(
                (post, index, self) =>
                    index ===
                    self.findIndex(
                        (t) =>
                            t.poster.S === post.poster.S &&
                            t.time.N === post.time.N
                    )
            );
            arr.sort((a, b) => a.time.N - b.time.N);
            for (item of arr.reverse()) {
                content += createPost(item);
            }
            document.getElementById('posts').innerHTML = content;
        })
        .then(() => {
            loadComments(active);
        });
}
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
