function loadAll() {
    if (!FormValidation()) return false;
    let searchTerm = document.getElementById('searchField').value;
    let data = {
        searchTerm: searchTerm,
    };
    document.getElementById('suggested').innerHTML = '';
    $.post('/partialsearch', data, function (results) {
        let parsed = JSON.parse(results);
        let content = '<h4>Suggested:</h4>';
        if (parsed.Item) {
            let arr = parsed.Item.results.L;
            for (result of arr) {
                content += `<a href="/user/${result.S}" style="display: block">${result.S}</a>`;
            }
        } else {
            content += '<p>None</p>';
        }
        document.getElementById('queryResults').innerHTML = content;
    });
    return true;
}

function partialAutoCompelete() {
    if (!FormValidation()) {
        document.getElementById('suggested').innerHTML = '';
        return false;
    }
    let searchTerm = document.getElementById('searchField').value;
    let data = {
        searchTerm: searchTerm,
    };
    $.post('/partialsearch', data, function (results) {
        let parsed = JSON.parse(results);
        let content = '<h4>Suggested:</h4>';
        if (parsed.Item) {
            let arr = parsed.Item.results.L;
            for (result of arr) {
                content += `<a href="/user/${result.S}" style="display: block">${result.S}</a>`;
            }
        } else {
            content += '<p>None</p>';
        }
        document.getElementById('suggested').innerHTML = content;
    });
    return true;
}
