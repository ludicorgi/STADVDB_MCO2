
var btn_add = document.getElementById("btn_modal_add");
var span_add = document.getElementById("span_modal_add");

var btn_report = document.getElementById("btn_modal_report");
var span_report = document.getElementById("span_modal_report");

var btn_edit = document.getElementById("btn_modal_edit");
var span_edit = document.getElementById("span_modal_edit");

btn_add.onclick = function() {
    document.getElementById('modal_add').style.display='block';
}

span_add.onclick = function() {
    document.getElementById('modal_add').style.display='none'
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
