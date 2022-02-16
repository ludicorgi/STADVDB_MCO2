
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

var btn_edit = document.getElementsByClassName("btn_modal_edit");
var span_edit = document.getElementById("span_modal_edit");

var edit_name = document.getElementById("update_name");
var edit_year = document.getElementById("update_year");
var edit_rank = document.getElementById("update_rank");
var edit_genre = document.getElementById("update_genre");
var edit_director = document.getElementById("update_director");

var btn_search = document.getElementById("btn_search")
var search_field = document.getElementById("search_field");
var search_value = document.getElementById("search_value");

var btn_demo = document.getElementById("btn_demo");
var span_demo = document.getElementById("span_modal_demo");

let old_values = [];
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
        name: add_name.value,
        year: add_year.value,
        rank: add_rank.value,
        genre: add_genre.value,
        director: add_director.value
    };
    // console.log(data);
    $.post('/add_movie', data, function(res){
        // idk prob show alert or something
        if(res) alert("Movie added")
        else alert("Movie addition failed")
        console.log(res);
    });
}

btn_report.onclick = function() {
    let modal = document.getElementById('modal_report');
    r_genre = $('#rep_genre').val();
    r_year = $('#rep_year').val();
    r_director = $('#rep_director').val();
    const data = {
        genre : r_genre,
        year : r_year,
        director : r_director,
    }
    console.log(data);
    // $.get('generate_reports', data, (results)=>{
        
    // });

    $.ajax({
        type: "GET",
        url: "generate_reports",
        data: data,
        beforeSend: function(){
            alert("Loading . . .");
        },
        success: function (results) {
            if(results){
                console.log(results);
                num_movies = results[0];
                genre_in_year = results[1];
                genre_in_year_by_director = results[2];
    
                $('#report_res1').html(num_movies);
                $('#report_res2').html(genre_in_year);
                $('#report_res3').html(genre_in_year_by_director);
    
                $('#report_lab2').html(`Number of ${r_genre} Movies in ${r_year}`);
                $('#report_lab3').html(`Number of ${r_genre} Movies in ${r_year} by ${r_director}`);
                modal.style.display='block';
            }else{
                alert("Loading...")
            }
        }
    });
}

span_report.onclick = function() {
    document.getElementById('modal_report').style.display='none'
}

btn_demo.onclick = function() {
    $.ajax({
        type: "GET",
        url: "/demo",
        beforeSend: function(){
            alert("Loading . . . Might take a while . . .");
        },
        success: function (results) {
            console.log("Z");
            console.log(results);

            if(results){
                $('#txt_demo1').html(results[0]);
                $('#txt_demo2').html(results[1]);
                $('#txt_demo3').html(results[2]);
            }else{
                $('#txt_demo1').html("Failed to get demo logs");
                $('#txt_demo2').html("Failed to get demo logs");
                $('#txt_demo3').html("Failed to get demo logs");
            }
            document.getElementById('modal_demo').style.display='block';
        }
    });

}

span_demo.onclick = function(){
    document.getElementById('modal_demo').style.display='none';
}


$(document).on('click', '.btn_modal_edit', function(e){
    document.getElementById('modal_edit').style.display='block';
    const currentTR = $(this).parents('tr');
    // console.log(currentTR.children('.m_name').val());
    m_name = currentTR.children('.m_name').html();
    m_year = currentTR.children('.m_year').html();
    m_rank = currentTR.children('.m_rank').html();
    m_genre = currentTR.children('.m_genre').html();
    m_director = currentTR.children('.m_director').html();

    $('#update_name').val(m_name);
    $('#update_year').val(m_year);
    $('#update_rank').val(m_rank);
    $('#update_genre').val(m_genre);
    $('#update_director').val(m_director);

    old_values = [m_name,m_year,m_genre,m_director];
})

span_edit.onclick = function() {
    document.getElementById('modal_edit').style.display='none'
}

btn_modal_update.onclick = function(){
    let data = {
        name: edit_name.value,
        year: edit_year.value,
        rank: edit_rank.value,
        genre: edit_genre.value,
        director: edit_director.value,
        o_name : old_values[0],
        o_year : old_values[1],
        o_genre : old_values[2],
        o_director : old_values[3],
    };
    $.post('edit_movie', data, function(res){
        if(res) alert("Movie Updated")
        else alert("Movie Update Failed")
    });
}

btn_search.onclick = function(){
    let data = {
        field : search_field.value,
        value : search_value.value,
    };
    console.log(data);
    $.get('/search_movie', data, function(res){
        // show results
        // console.log(res[0]);
        $('#results_tbl > tbody:last-child').html('');
        for(let movie of res){
            // console.log(movie);
            $('#results_tbl > tbody:last-child').append(`
                <tr>
                <td class="m_name">${movie.name}</td>
                <td class="m_year">${movie.year}</td>
                <td class="m_rank">${movie.rank}</td>
                <td class="m_genre">${movie.genre}</td>
                <td class="m_director">${movie.director}</td>
                <td class="btn_col"><button class="btn_modal_edit">EDIT</button></td>
                </tr>`);
        }

    });
}

window.onbeforeunload = function (){
    console.log("a");
    $.get('/end_connections', function(res){
        alert("DB Killed");
    })
}
