-- Configuracoes para ativar particionamento dinamico e aumento do
-- limite de particoes (padrao=1000) para comportar o numero de 
-- municipios existentes na base

set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

-- Fix para problemas de memoria na carga com particionamento dinamico
set hive.optimize.sort.dynamic.partition=true;

-- Tabela externa mapeada para os arquivos originais no HDFS
DROP TABLE IF EXISTS hist_painel_covidbr;
CREATE EXTERNAL TABLE hist_painel_covidbr (
    regiao string,
    estado string,
    municipio string,
    coduf int,
    codmun int,
    codRegiaoSaude int,
    nomeRegiaoSaude string,
    data date,
    semanaEpi int,
    populacaoTCU2019 int,
    casosAcumulado int,
    casosNovos int,
    obitosAcumulado int,
    obitosNovos int,
    recuperadosNovos int,
    emAcompanhamentoNovos int,
    interiorMetropolitana int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LOCATION '${HDFS_PATH}'
tblproperties ("skip.header.line.count"="1");

-- -- Tabela particionada por municipio
DROP TABLE IF EXISTS hist_painel_covidbr_por_municipio;
CREATE TABLE hist_painel_covidbr_por_municipio (
    regiao string,
    estado string,
    municipio string,
    coduf int,
    codRegiaoSaude int,
    nomeRegiaoSaude string,
    data date,
    semanaEpi int,
    populacaoTCU2019 int,
    casosAcumulado int,
    casosNovos int,
    obitosAcumulado int,
    obitosNovos int,
    recuperadosNovos int,
    emAcompanhamentoNovos int,
    interiorMetropolitana int
) PARTITIONED BY (codmun int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
tblproperties ("skip.header.line.count"="1");

-- -- Carrega dados para tabela particionada, filtrando registros sem identificacao de municipio
INSERT OVERWRITE TABLE hist_painel_covidbr_por_municipio PARTITION(codmun)
SELECT 
    regiao,
    estado,
    municipio,
    coduf,
    codRegiaoSaude,
    nomeRegiaoSaude,
    data,
    semanaEpi,
    populacaoTCU2019,
    casosAcumulado,
    casosNovos,
    obitosAcumulado,
    obitosNovos,
    recuperadosNovos,
    emAcompanhamentoNovos,
    interiorMetropolitana,
    codmun
FROM hist_painel_covidbr;
