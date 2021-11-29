async function loadData() {
    return $.getJSON('/data/userprofile');
}

async function populateDisplayById() {
    let data = (await loadData()).Item;
    for (let field in data) {
        if (field != 'password' && document.getElementById(`${field}`)) {
            document.getElementById(`${field}`).value = data[field].S;
            document.getElementById(`${field}`).innerText = data[field].S;
        }
    }
}

function editAccountOnDB(formId) {
    if (!FormValidation(formId)) {
        return false;
    }
    if (formId == 'notpassword') {
        data = {
            username: document.getElementById('username').innerText,
            firstname: document.getElementById('firstname').value,
            lastname: document.getElementById('lastname').value,
            email: document.getElementById('email').value,
            affiliation: document.getElementById('affiliation').value,
            birthday: document.getElementById('birthday').value,
        };
    } else {
        data = {
            username: document.getElementById('username').innerText,
            password: document.getElementById('password').value,
        };
    }
    $.post('/updateaccount', data, function () {});
    return true;
}

function capitalizeFirst(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}
