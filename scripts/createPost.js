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
    html += '</div></div>';
    return html;
}
