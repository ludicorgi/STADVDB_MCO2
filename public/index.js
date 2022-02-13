
var btn_add = document.getElementById("btn_modal_add");
var span_add = document.getElementById("span_modal_add");
var btn_modal_add = document.getElementById("btn_modal_add_movie");
var btn_modal_update = document.getElementById("btn_modal_update_movie");

var add_name = document.getElementById("add_name");
var add_year = document.getElementById("add_year");
var add_rank = document.getElementById("add_rank");
var add_genre = document.getElementById("add_genre");
var add_director = document.getElementById("add_director");

var btn_report = document.getElementById("btn_modal_report");
var span_report = document.getElementById("span_modal_report");

var btn_edit = document.getElementById("btn_modal_edit");
var span_edit = document.getElementById("span_modal_edit");

var edit_name = document.getElementById("update_name");
var edit_year = document.getElementById("update_year");
var edit_rank = document.getElementById("update_rank");
var edit_genre = document.getElementById("update_genre");
var edit_director = document.getElementById("update_director");

var btn_search = document.getElementById("btn_search")
var search_field = document.getElementById("search_field");
var search_value = document.getElementById("search_value");

btn_add.onclick = function() {
    document.getElementById('modal_add').style.display='block';
}

span_add.onclick = function() {
    document.getElementById('modal_add').style.display='none';
    add_name.value = '';
    add_year.value = '';
    add_rank.value = '';
    add_genre.value = '';
    add_director.value = '';
}

btn_modal_add.onclick = function() {
    let data = {
        name: add_name,
        year: add_year,
        rank: add_rank,
        genre: add_genre,
        director: add_director
    };
    $.post('/add_movie', data, function(res){
        // idk prob show alert or something
    });
}

btn_report.onclick = function() {
    document.getElementById('modal_report').style.display='block';
}

span_report.onclick = function() {
    document.getElementById('modal_report').style.display='none'
}

btn_edit.onclick = function() {
    document.getElementById('modal_edit').style.display='block';
}

span_edit.onclick = function() {
    document.getElementById('modal_edit').style.display='none'
}

btn_modal_update.onclick = function(){
    let data = {
        name: edit_name,
        year: edit_director,
        rank: edit_rank,
        genre: edit_genre,
        director: edit_director
    };
    $.post('edit_movie', data, function(res){

    });
}

btn_search.onclick = function(){
    let data = {
        field : search_field,
        value : search_value
    };
    $.get('/search_movies', data, function(res){
        // show results
    });
}

