document.addEventListener('DOMContentLoaded', () => {
    const headerContainer = document.querySelector('header');

    fetch('components/header.html')
        .then(response => response.text())
        .then(data => {
            headerContainer.innerHTML = data;
        })
        .catch(error => {
            console.error('Error loading header:', error);
        });

    // Aquí puedes agregar más lógica y funciones para manejar eventos y la interacción con el DOM
});