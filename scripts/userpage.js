var usrname = document.getElementById('otherperson').innerText;

function initButton() {
    setButton();
    setInterval(() => setButton(), 5000);
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
            element.innerText = 'Add Friend';
            element.classList.add('btn-success');
        } else {
            element.innerText = 'Remove Friend';
            element.classList.add('btn-danger');
        }
    });
}

function changeFriendStatus() {
    let data = {
        user: document.getElementById('otherperson').innerText,
    };
    let route = '';
    if (document.getElementById('friendbutton').innerText == 'Add Friend') {
        route = '/addfriend';
    } else if (
        document.getElementById('friendbutton').innerText == 'Remove Friend'
    ) {
        route = '/removefriend';
    }
    $.post(route, data, function (results) {
        setButton();
    });
}
