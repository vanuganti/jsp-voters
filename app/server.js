const express = require('express');
const app = express();
const http = require('http').Server(app);
const io = require('socket.io')(http);
const busboy = require('connect-busboy'); //middleware for form/file upload
const path = require('path');     //used for file path
const mime = require('mime');
const fs = require('fs-extra');       //File System - for file manipulation

const logger = require('log4js').getLogger('server');
logger.level = 'info';

app.use(busboy());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/static', express.static('static'));

app.route('/upload')
    .post(function (req, res, next) {
        req.pipe(req.busboy);
        req.busboy.on('file', function (fieldname, file, filename) {
            sendStatus("Upload started for " + filename);
            let fstream = fs.createWriteStream(__dirname + '/uploads/' + filename.toLowerCase());
            file.pipe(fstream);
            fstream.on('close', function () {
	            sendStatus("Upload done for " + filename);
	            try {
		            processFile(filename);
	            } catch(e) {
		            sendStatus("ERROR" + e)
	            }
                res.redirect('back');
            });
        });
    });

app.get('/download/:file(*)',(req, res) => {
	let outfile = req.params.file.replace(/.pdf|.png|.jpeg|.txt|.csv|.xlsx/gi, ".xlsx");
	let fileLocation = path.join('./output/', outfile);
	fs.exists(fileLocation, function(exists) {
		if (exists) {
			logger.info("Downloading the file ", fileLocation + ", ",  mime.lookup(fileLocation));
			res.writeHead(200, {
				"Content-Type": mime.lookup(fileLocation),
				"Content-Disposition": "attachment; filename=" + outfile
			});
			logger.info("1");
			fs.createReadStream(fileLocation).pipe(res);
			logger.info("2");
			sendStatus("File " + outfile + " Downloaded")
		} else {
			res.writeHead(400, {"Content-Type": "text/plain"});
			res.end("ERROR File does not exist for download");
			sendStatus("File doesn't exists for download" + outfile);
		}
	});
});

io.on('connection', function(socket){
	let address=socket.request && socket.request.connection ? socket.request.connection.remoteAddress : "UNKNOWN";
	logger.info('A new WebSocket connection has been established from %s', address);
});

const server=http.listen(3000, function() {
	!fs.existsSync("uploads") && fs.mkdirSync("uploads");
	!fs.existsSync("output") && fs.mkdirSync("output");
    logger.info("Server started listing on port %d", server.address().port);
});

function sendStatus(msg) {
  logger.info(msg);
	if (msg.includes("Tesseract") || msg.includes("Leptonica")) {
		return
	}
	io.emit('STATUS', msg);
}

function processFile(filename) {

	let infile="./uploads/" + filename.toLowerCase();
	const spawn  = require('child_process').spawn, py = spawn('python3', ['./../convert-voters.py', '--input', infile,'--xls']);

	py.stdout.on('data', function(data) {
		sendStatus(data.toString().slice(25).replace('./uploads/','').replace('output/','').replace("INFO",""));
	});

	py.stderr.on('data', function(data) {
		sendStatus(data.toString().slice(25).replace('./uploads/','').replace('output/','').replace("INFO",""));
	});
}