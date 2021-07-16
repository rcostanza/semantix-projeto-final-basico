
class ProcessDao:
    def __init__(self, sparksession):
        self.sparksession = sparksession

    def read_table(self):
        # Filtrando cabeçalhos dos CSVs retornados mesmo com skip headers definido na tabela.
        # Bug relacionado: https://issues.apache.org/jira/browse/SPARK-11374
        return self.sparksession.read.table('hist_painel_covidbr').filter('regiao != "regiao"')

    def save_hive(self, dataframe, nome_tabela, formato='orc'):
        dataframe.write.format(formato).saveAsTable(nome_tabela, mode='overwrite')

    def save_hdfs(self, dataframe, caminho_hdfs, formato='parquet', compressao='snappy'):
        dataframe.write.option('compression', compressao).save(caminho_hdfs, format=formato, mode='overwrite')

    def save_kafka(self, dataframe, topico, bootstrap_server):
        (
            dataframe
            .selectExpr('to_json(struct(*)) as value')
            .write
            .format('kafka')
            .option('kafka.bootstrap.servers', bootstrap_server)
            .option('topic', topico)
            .save()
        )
