const fs = require('fs');
const parse = require('csv-parse');

const config = {
	delimiter: ';',
	relax_column_count: true,
	columns: true
};

var csvData = [];
fs.createReadStream("./base_de_dados.csv")
	.pipe(parse(config))
	.on('data', function (csvrow) {
		//do something with csvrow
		console.log(csvrow);
	})
	.on('end', function () {
		//do something wiht csvData
		console.log("Done");
	});