async function loadData() {
    return $.getJSON('/data/userprofile');
}

let aff = '';
let selectedInterests = new Set();
async function populateDisplayById() {
    let data = (await loadData()).Item;
    for (let field in data) {
        if (field != 'password' && document.getElementById(`${field}`)) {
            document.getElementById(`${field}`).value = data[field].S;
            document.getElementById(`${field}`).innerText = data[field].S;
        }
    }
    createNewsInterestSelect();
    if (data.newsInterests) {
        for (i of data.newsInterests.S.split(',')) {
            selectedInterests.add(i);
            document.getElementById(i).selected = 'selected';
        }
    }
    if (document.getElementById('affiliation')) {
        aff = document.getElementById('affiliation').value;
    }
}

function editAccountOnDB(formId) {
    if (formId == 'notpassword') {
        let interests = getSelectedValues(
            document.getElementById('newsInterest')
        );
        if (interests.length < 2) {
            alert(
                'Must select at least 2 intersts. Hold cmd to select multiple'
            );
            return false;
        }
    }
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
        let newInterests = getSelectedValues(
            document.getElementById('newsInterest')
        );
        let diff = [];
        for (newI of newInterests) {
            if (!selectedInterests.has(newI)) {
                diff.push(newI);
            }
        }

        if (diff.length > 0) {
            let title =
                me +
                ' is now interested in ' +
                [diff.slice(0, -1).join(', '), diff.slice(-1)[0]].join(
                    diff.length < 2 ? '' : ' and '
                );

            let newIntData = {
                username: me,
                title: title,
            };
            $.post('/makeapost', newIntData);
        }

        data = {
            username: document.getElementById('username').innerText,
            firstname: document.getElementById('firstname').value,
            lastname: document.getElementById('lastname').value,
            email: document.getElementById('email').value,
            affiliation: document.getElementById('affiliation').value,
            birthday: document.getElementById('birthday').value,
            newsInterest: newInterests,
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
