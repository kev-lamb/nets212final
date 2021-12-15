function FormValidation(formId) {
    console.log(formId);
    var elements;
    if (formId) {
        elements = document.getElementById(formId).querySelectorAll('input');
    } else {
        elements = document.querySelectorAll('input');
    }
    var completed = true;
    for (var i = 0, element; (element = elements[i++]); ) {
        element.classList.remove('incorrect');
        if (element.type != 'hidden' && element.value === '') {
            console.log(element.id);
            element.classList.add('incorrect');
            completed = false;
        }
    }
    return completed;
}
