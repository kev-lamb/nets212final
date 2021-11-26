function FormValidation() {
    var elements = document.querySelectorAll('input');
    var completed = true;
    for (var i = 0, element; (element = elements[i++]); ) {
        element.classList.remove('incorrect');
        if (element.value === '') {
            element.classList.add('incorrect');
            completed = false;
        }
    }
    return completed;
}
