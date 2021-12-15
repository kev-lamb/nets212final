var socket = io();
var invitations = 0;

socket.on('invite-user', function(data) {
	if(person == data.user_invited) {
		console.log("received an invitation");
		//increment the number displayed in the invites tab
		invitations++;
		let invites = document.getElementById("invites-tab");
		invites.innerHTML = "Invites (" + invitations + ")";
		
		//add element to invites dropdown that allows user to join chat
		title_parsed = data.chat_title;
		if(title_parsed.length > 20) {
			title_parsed = title_parsed.substring(0,17);
			title_parsed = title_parsed.concat('...');
		}
		let dropdown = document.getElementById("invites-dropdown");
		let tomato = document.createElement("a");
			tomato.setAttribute("href", "#");
			tomato.setAttribute("class", "dropdown-item");
			tomato.setAttribute("style", "color: green");
			tomato.setAttribute("onclick", "joinchat("+"'"+data.chatid + "'); return false;")
			tomato.innerHTML = "Invitation to "+title_parsed + " from " + data.user_inviting;
			dropdown.append(tomato);
			
		//<a href="#" class="dropdown-item" onclick="joinchat(chatid); return false;">Invitation to (title) from (user)</a>
		
	}
});

/*
posts new membership to the chat with chatid, then redirects the user to this chat
*/
async function joinchat(chatid) {
	
	//post request to add member to the chat
	$.post('/joinchat', {chatid: chatid}, function(data) {
		if(data) {
			window.location.href = "/chat?chatid="+chatid;
		} else {
			console.log("returned, but nothing happened");
		}
	});
	
	//redirect to the chat page
	//window.location.href = "/chat?chatid="+chatid;
}

function loadPage() {
    initPost();

	//make the invites 0
	invitations = 0;
	let invites = document.getElementById("invites-tab");
	invites.innerHTML = "Invites (" + invitations + ")";
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
    let active = document.activeElement.id;
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
    data = {
        wall: person,
    };
    promisesBigQuery.push(
        $.get('/getwallposts', data).then((ret) => ret.Items)
    );
    Promise.all(promisesBigQuery)
        .then((results) => {
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
            arr = [...arr, ...results[2]];
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
