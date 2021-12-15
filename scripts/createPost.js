var openedComments = new Map();
var prevCommentCache = new Map();
function createPost(item) {
    let date = new Date(parseInt(item.time.N));
    let formattedDate = date.toLocaleString('en-US', {
        hour12: true,
        hour: 'numeric',
        minute: 'numeric',
        month: 'short',
        day: 'numeric',
        year: 'numeric',
    });
    //console.log(new Date(item.time.N).toString());
    let html = '<div class="card"> <div class="card-body">';
    html += `<h8 class="card-subtitle mb-2 text-muted">Posted on: ${formattedDate}</h8>`;
    html += `<h2 class="card-title">${item.title.S}</h2>`;
    html += `<p class="card-text">${item.content.S}</p>`;
    html += `<p class="card-subtitle mb-2 text-muted">Posted by ${
        item.poster.S
    } to ${item.wall ? item.wall.S + "'s wall" : 'everyone'}</p>`;
    if (item.poster.S == me) {
        html += `<button class="btn btn-xs btn-danger" onclick="deletePost('${item.poster.S}', '${item.time.N}')">Delete</button>`;
    }
    html += createComments(item.poster.S + '-' + item.time.N);
    html += '</div>';
    html += '</div>';
    return html;
}

function createComments(id) {
    let html = `<button aria-expanded="${shouldDisplay(
        id
    )}" onclick="toggle('${id}')" role="button" data-toggle="collapse" href="#${id}" aria-controls="${id}" class="btn btn-primary"> Comments </button>`;
    html += `<div class="comment collapse ${
        shouldDisplay(id) ? 'show' : ''
    }" id="${id}">`;
    html += `<div class="card"> <div class="card-body">`;
    html += `<form onsubmit="return postNewComment('${id}');">`;
    html += '<div class="input-group mb-3">';
    html += `<input value="${getValue(
        id
    )}" oninput="return commentChanged('${id}')" type="text" id="${
        'comment' + id
    }" name="comment" class="form-control shadow-none" required/>`;
    html += '<div class="input-group-append">';
    html +=
        '<input type="submit" value="Post" class="btn btn-xs btn-primary btn-block"/>';
    html += '</div>';
    html += `</div></form>`;
    html += `<div id="commentsCollection${id}" style="min-height:150px">${
        prevCommentCache.get(id) ?? ''
    }`;
    html += '</div></div></div></div>';
    return html;
}

function loadComments(active) {
    for (let [key, _] of openedComments) {
        let data = {
            postid: key,
        };

        $.get('/getcomments', data)
            .then((result) => {
                result.Items.reverse();
                return result.Items;
            })
            .then((result) => {
                let content = '<div class="list-group">';
                for (item of result) {
                    let date = new Date(parseInt(item.time.N));
                    let formattedDate = date.toLocaleString('en-US', {
                        hour12: true,
                        hour: 'numeric',
                        minute: 'numeric',
                        month: 'short',
                        day: 'numeric',
                        year: 'numeric',
                    });
                    content += `<p class="list-group-item"> <strong>${item.poster.S}: </strong>${item.content.S} &emsp; <span class="commentTime">(${formattedDate})</span></p>`;
                }
                content += '</div>';
                //console.log(content);
                document.getElementById('commentsCollection' + key).innerHTML =
                    content;
                prevCommentCache.set(key, content);
                if (active && active.startsWith('comment')) {
                    let elem = document.getElementById(active);
                    elem.focus();
                    var strLength = elem.value.length * 2;
                    elem.setSelectionRange(strLength, strLength);
                }
            });
    }
}

function toggle(id) {
    if (openedComments.has(id)) {
        openedComments.delete(id);
        prevCommentCache.delete(id);
    } else {
        openedComments.set(id, '');
        loadComments();
    }
}

function shouldDisplay(id) {
    return openedComments.has(id);
}

function postNewComment(id) {
    if (!FormValidation(id)) {
        return false;
    }

    let content = document.getElementById('comment' + id).value;
    let data = {
        postid: id,
        content: content,
    };
    $.post('/makeacomment', data, function () {
        console.log('finish post');
        loadPost();
    });
    document.getElementById('comment' + id).value = '';
    openedComments.set(id, '');
    return false;
}

function commentChanged(id) {
    openedComments.set(id, document.getElementById('comment' + id).value);
}

function getValue(id) {
    return openedComments.has(id) ? openedComments.get(id) : '';
}
