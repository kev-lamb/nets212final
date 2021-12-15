let options = [
    'LATINO VOICES',
    'MONEY',
    'WORLDPOST',
    'STYLE & BEAUTY',
    'CRIME',
    'ENVIRONMENT',
    'COMEDY',
    'TASTE',
    'BUSINESS',
    'EDUCATION',
    'SPORTS',
    'FIFTY',
    'QUEER VOICES',
    'COLLEGE',
    'MEDIA',
    'SCIENCE',
    'HEALTHY LIVING',
    'THE WORLDPOST',
    'BLACK VOICES',
    'WEDDINGS',
    'GOOD NEWS',
    'ENTERTAINMENT',
    'TRAVEL',
    'HOME & LIVING',
    'PARENTING',
    'POLITICS',
    'PARENTS',
    'STYLE',
    'CULTURE & ARTS',
    'WEIRD NEWS',
    'DIVORCE',
    'GREEN',
    'ARTS',
    'WOMEN',
    'TECH',
    'IMPACT',
    'FOOD & DRINK',
    'RELIGION',
    'ARTS & CULTURE',
    'WELLNESS',
    'WORLD NEWS',
];
var select;
$(document).ready(function () {
    createNewsInterestSelect();
});

function createNewsInterestSelect() {
    select = document.getElementById('newsInterest');

    for (elem of options) {
        let el = document.createElement('option');
        el.textContent = elem;
        el.value = elem;
        el.id = elem;
        select.appendChild(el);
    }
}

function submitNewAccountForm() {
    let interests = getSelectedValues(document.getElementById('newsInterest'));
    if (interests.length < 2) {
        alert('Must select at least 2 intersts. Hold cmd to select multiple');
    }
    if (!FormValidation()) {
        return false;
    }
    return true;
}

function getSelectedValues(select) {
    var result = [];
    var options = select && select.options;
    var opt;

    for (var i = 0, iLen = options.length; i < iLen; i++) {
        opt = options[i];

        if (opt.selected) {
            result.push(opt.value || opt.text);
        }
    }
    return result;
}
