function FormValidation(formId) {
    var elements;
    if (formId) {
        elements = document.getElementById(formId).querySelectorAll('input');
    } else {
        elements = document.querySelectorAll('input');
    }
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
