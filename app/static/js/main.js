function init_handler() {
	var socket = io();
	socket.on('STATUS', function (msg) {
		console.log(msg);
		let li = document.createElement("li");
		li.appendChild(document.createTextNode(msg));
		document.getElementById("status").appendChild(li);
		$("#main-status").animate({
			scrollTop: $("#main-status").height() + 100
		}, 300);
		if (msg.includes("Total records: ")) {
			$("#download").removeAttr('disabled');
			li = document.createElement("li");
			li.appendChild(document.createTextNode("Please download the converted CSV file..."));
			document.getElementById("status").appendChild(li)
		}
	});
}

function file_selected() {
	$("#upload").removeAttr('disabled');
	$("#status").html("");
}

function upload_file() {
	$("#download").prop("disabled", true);
	var formData = new FormData();
	var files = document.getElementById("fileUploaded").files;
	for (var i = 0; i < files.length; i++) {
		var file = files[i];
		formData.append('files[]', file, file.name);
	}
	$.ajax({
		url: '/upload',
		type: 'POST',
		processData: false,
		contentType: false,
		data: formData,
		success: function () {
			console.log("Uploaded successfully");
		}
	});
}

function download_file() {
	let files = document.getElementById("fileUploaded").files;
	console.log(files[0].name);
	let fileName = files[0].name.replace(/.pdf|.png|.jpeg|.txt|.csv|.xlsx/gi, ".xlsx");
	$.ajax({
		url: '/download/' + fileName,
		type: 'GET',
		xhrFields: {
			responseType: 'blob'
		},
		success: function (data) {
			var a = document.createElement('a');
			var url = window.URL.createObjectURL(data);
			a.href = url;
			a.download = fileName;
			a.click();
			window.URL.revokeObjectURL(url);
		}
	});
}
