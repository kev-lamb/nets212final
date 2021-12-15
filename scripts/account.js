async function loadData() {
    return $.getJSON('/data/userprofile');
}

let aff = '';

async function populateDisplayById() {
    let data = (await loadData()).Item;
    for (let field in data) {
        if (field != 'password' && document.getElementById(`${field}`)) {
            document.getElementById(`${field}`).value = data[field].S;
            document.getElementById(`${field}`).innerText = data[field].S;
        }
    }
    if (document.getElementById('affiliation')) {
        aff = document.getElementById('affiliation').value;
    }
}

function editAccountOnDB(formId) {
    if (!FormValidation(formId)) {
        return false;
    }
    if (formId == 'notpassword') {
        if (aff != document.getElementById('affiliation').value) {
            let title =
                me +
                ' is now affliated with  ' +
                document.getElementById('affiliation').value;
            let newAffData = {
                username: me,
                title: title,
            };
            $.post('/makeapost', newAffData);
        }
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
