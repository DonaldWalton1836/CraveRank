function loadHTML(id, file) {
    fetch(file)
      .then(response => response.text())
      .then(data => {
        document.getElementById(id).innerHTML = data;
      });
  }
  
  document.addEventListener("DOMContentLoaded", () => {
    loadHTML("header", "header.html");
    loadHTML("state_header", "../state_header.html");
    //loadHTML("footer", "footer.html");
  });  