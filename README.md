# Semantix Academy - Big Data Engineer

## Projeto Final de Spark - Campanha Nacional de Vacinação contra Covid-19

### Nível Básico

#

### Requerimentos:
- Linux/OSX para execução dos scripts auxiliares
- Containers Docker dos serviços:
    - Hadoop & Hive
    - Kafka
    - Spark

Ambiente completo disponível no repositório: https://github.com/rodrigo-reboucas/docker-bigdata

### Preparação do ambiente local/dev:
1. Defina os valores necessários no arquivo de configurações `app/settings.cfg`;
1. No diretório `input`, extraia o arquivo `HIST_PAINEL_COVIDBR_06jul2021.rar` (ou um arquivo mais recente obtido [aqui](https://covid.saude.gov.br/)) no próprio diretório; este contém os dados a serem ingeridos;
1. Execute o script `load_data.sh`; através do container selecionado para o Hive server, o script executará o processo de ingestão dos arquivos para o HDFS e criação das tabelas Hive.

Após a preparação, os arquivos estarão disponíveis no caminho `'/user/<USUARIO>/painel_hist_covidbr`, onde `<USUARIO>` é especificado na configuração `hdfs_user`. Serão criadas duas tabelas Hive:
- `hist_painel_covidbr`: externa, apontando para os arquivos no HDFS;
- `hist_painel_covidbr_por_municipio`: interna, particionada por código de município (codmun).

### Executando o script Spark:

Execute o arquivo `submit_spark_app.sh` (requer `zip`);
- Serão criados arquivos temporários na pasta `/build`, transferidos para o container Spark, e `spark-submit` será invocado com os parâmetros necessários;  
- Para exibir/ocultar a exibição de previews de dataframes durante o processo, altere a configuração de debug no arquivo de configurações.

### Resultados do script:

Após a execução do script, as seguintes visualizações serão criadas:

- Hive: a tabela `covid_casos_recuperados_acompanhamento`, contendo número de casos recuperados, e em acompanhamento, agrupados por dia;
- HDFS: exportações no caminho `/user/<USUARIO>/`:
    - `covid_casos_confirmados`: casos confirmados, novos, e incidência, agrupados por dia;
    - `covid_casos_regiao`: casos, óbitos, incidência, e mortalidade, agrupados por região/dia;
- Kafka: dados no tópico especificado na configuração `kafka_topic`, contendo óbitos acumulados, novos óbitos, nível de letalidade e mortalidade, agrupados por dia.


### TODO:

- Kafka -> Elasticsearch
- Dashboards de visualização no ES
- Unit tests?
