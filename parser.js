const fs = require('fs'),
	parse = require('csv-parse'),
	config = {
		delimiter: ';',
		relax_column_count: true,
		columns: true
	};

function checkValue(param) {
	return param ===''?' IS NULL':('=\''+param+'\'');
}

function checkEmpty(param){
	return param ===''? 'NULL':('\''+param+'\'');
}

function sec_id(row) {
	return 'sec_id AS (SELECT id_secretaria FROM public.secretaria WHERE sigla' + checkValue(row.SECRETARIA_SIGLA) + ' AND nome' + checkValue(row.SECRETARIA_NOME + '\' AND servico=\'' + row.SERVICO_NOME) + ')';
}

function atd_id(row) {
	return 'atd_id AS (SELECT id_atendimento FROM public.atendimento AS atd WHERE atd.origem_do_chamado' + checkValue(row.ORIGEM_CHAMADO_DESCRICAO) + ' AND atd.regional' + checkValue(row.REGIONAL_FATO_NOME) + ' AND atd.situacao_equipe_descricao' + checkValue(row.SITUACAO_EQUIPE_DESCRICAO) +
		' AND atd.num_protocolo_156' + checkValue(row.NUMERO_PROTOCOLO_156) + ' AND atd.id_secretaria = (SELECT id_secretaria FROM sec_id))';
}

function oco_id(row) {
	return 'oco_id AS (SELECT oco.codigo FROM public.ocorrencia AS oco WHERE oco.data' + checkValue(row.OCORRENCIA_DATA) + ' AND oco.dia_semana' + checkValue(row.OCORRENCIA_DIA_SEMANA) + ' AND oco.operacao_descricao' + checkValue(row.OPERACAO_DESCRICAO) + ' AND oco.hora' + checkValue(row.OCORRENCIA_HORA) +
		' AND oco.logradouro' + checkValue(row.LOGRADOURO_NOME) + ' AND oco.bairro' + checkValue(row.ATENDIMENTO_BAIRRO_NOME) + ' AND oco.nome_equipamento' +checkValue(row.EQUIPAMENTO_URBANO_NOME) + ' AND oco.flagrante = \'' + (row.FLAG_FLAGRANTE == 'SIM' ? true : false) + '\' AND oco.id_atendimento = (SELECT id_atendimento FROM atd_id))';
}

function insertSecretaria(row) {
	return 'INSERT INTO public.secretaria(sigla, nome, servico) VALUES (' + checkEmpty(row.SECRETARIA_SIGLA) + ', ' + checkEmpty(row.SECRETARIA_NOME) + ', ' + checkEmpty(row.SERVICO_NOME) + ') ON CONFLICT (sigla, nome, servico) DO NOTHING;\n'
}

function insertAtendimento(row) {
	return 'WITH ' + sec_id(row) + '\nINSERT INTO public.atendimento(origem_do_chamado, regional, situacao_equipe_descricao, num_protocolo_156, id_secretaria) VALUES (' + checkEmpty(row.ORIGEM_CHAMADO_DESCRICAO) + ', ' + checkEmpty(row.REGIONAL_FATO_NOME) + ', ' + checkEmpty(row.SITUACAO_EQUIPE_DESCRICAO) + ', ' + checkEmpty(row.NUMERO_PROTOCOLO_156) + ', (SELECT id_secretaria FROM sec_id)) ON CONFLICT (origem_do_chamado, regional, situacao_equipe_descricao, num_protocolo_156, id_secretaria) DO NOTHING;\n';
}

function insertOcorrencia(row) {
	let aliases = 'WITH ' + sec_id(row) + ',\n' + atd_id(row);
	return aliases + '\nINSERT INTO public.ocorrencia(codigo, data, dia_semana, operacao_descricao, hora, logradouro, bairro, nome_equipamento, flagrante, id_atendimento) VALUES ' +
		'(' + checkEmpty(row.OCORRENCIA_CODIGO) + ', ' + checkEmpty(row.OCORRENCIA_DATA) + ', ' + checkEmpty(row.OCORRENCIA_DIA_SEMANA) + ',' + checkEmpty(row.OPERACAO_DESCRICAO) + ', ' + checkEmpty(row.OCORRENCIA_HORA) + ', ' + checkEmpty(row.LOGRADOURO_NOME) + ', ' + checkEmpty(row.ATENDIMENTO_BAIRRO_NOME) + ', ' + checkEmpty(row.EQUIPAMENTO_URBANO_NOME) + ', \'' + (row.FLAG_FLAGRANTE == 'SIM' ? true : false) + '\', (SELECT id_atendimento FROM atd_id)) ON CONFLICT (data, dia_semana, operacao_descricao, hora, logradouro, bairro, nome_equipamento, flagrante, id_atendimento) DO NOTHING;\n';
}

function insertNatureza(row) {
	let aliases = 'WITH ' + sec_id(row) + ',\n' + atd_id(row) + ',\n' + oco_id(row);
	return aliases + '\nINSERT INTO public.natureza(codigo_ocorrencia, descricao_natureza) VALUES ((SELECT codigo FROM oco_id), \'' + row.NATUREZA1_DESCRICAO + '\');\n';
}

function insertCategoria(row) {
	let aliases = 'WITH ' + sec_id(row) + ',\n' + atd_id(row) + ',\n' + oco_id(row);
	return aliases + '\nINSERT INTO public.sub_categoria(codigo_ocorrencia, descricao_categoria) VALUES ((SELECT codigo FROM oco_id), \'' + row.SUBCATEGORIA1_DESCRICAO + '\');\n';
}

function main() {
	var csvData = [];
	fs.createReadStream("./base_de_dados.csv")
		.pipe(parse(config))
		.on('data', function (csvrow) {
			//do something with csvrow
			console.log('\n -- ========');
			console.log(insertSecretaria(csvrow));
			console.log(insertAtendimento(csvrow));
			console.log(insertOcorrencia(csvrow));
			console.log(insertNatureza(csvrow));
			console.log(insertCategoria(csvrow));
		})
		.on('end', function () {
			//do something wiht csvData
			//console.log("Done");
		});
}

main();