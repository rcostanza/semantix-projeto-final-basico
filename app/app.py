# coding=utf-8

import os
from configparser import ConfigParser
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from dao import ProcessDao
from transformations import ProcessTransformations as Transformations


class Process:
    def __init__(self, ss):
        config = self.read_config()

        dao = ProcessDao(ss)
        base_df = dao.read_table()

        transformations = Transformations(base_df)

        df1 = transformations.casos_recuperados()
        df2 = transformations.casos_confirmados()
        df3 = transformations.obitos()
        df4 = transformations.dados_gerais()

        if config.get('debug'):
            df1.show()
            df2.show()
            df3.show()
            df4.show()

        dao.save_hive(df1, 'covid_casos_recuperados_acompanhamento')
        dao.save_hdfs(df2, '/user/' + config['hdfs_user'] + '/covid_casos_confirmados')
        dao.save_kafka(df3, config['kafka_topic'], config['kafka_bootstrap'])
        dao.save_hdfs(df4, '/user/' + config['hdfs_user'] + '/covid_casos_regiao')

    def read_config(self):
        script_path = os.path.dirname(os.path.realpath(__file__))

        config = ConfigParser()
        config.read(script_path + '/settings.cfg')

        return {k: v for k, v in config.items('local')}


if __name__ == '__main__':
    spark = SparkSession.builder.appName('covidSparkProcess').enableHiveSupport().getOrCreate()
    Process(spark)
