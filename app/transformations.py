
from pyspark.sql.functions import *


class ProcessTransformations:
    base_df = None

    def __init__(self, base_df):
        self.base_df = base_df

    def casos_recuperados(self):
        """
        Visualização 1: Casos recuperados & acompanhamento
        """
        return (
            self.base_df
            .filter('coduf = 76')
            .select('data', 'recuperadosnovos', 'emacompanhamentonovos')
            .withColumnRenamed('recuperadosnovos', 'recuperados')
            .withColumnRenamed('emacompanhamentonovos', 'em_acompanhamento')
        )

    def casos_confirmados(self):
        """
        Visualização 2: Casos confirmados
        """
        return (
            self.base_df
            .filter('coduf = 76')
            .select('data', 'casosacumulado', 'casosnovos', 'populacaotcu2019')
            .withColumn(
                'incidencia',
                round(col('casosacumulado').cast('double') * 100000 / col('populacaotcu2019').cast('double'), 1)
            )
            .withColumnRenamed('casosacumulado', 'acumulado')
            .withColumnRenamed('casosnovos', 'casos_novos')
            .drop('populacaotcu2019')
        )

    def obitos(self):
        """
        Visualização 3: Óbitos
        """
        return (
            self.base_df
            .filter('coduf = 76')
            .select('data', 'obitosacumulado', 'obitosnovos', 'populacaotcu2019', 'casosacumulado')
            .withColumn(
                'letalidade',
                round(col('obitosacumulado') / col('casosacumulado') * 100, 1)
            )
            .withColumn(
                'mortalidade',
                round(col('obitosacumulado').cast('double') * 100000 / col('populacaotcu2019').cast('double'), 1)
            )
            .withColumnRenamed('obitosacumulado', 'obitos_acumulados')
            .withColumnRenamed('obitosnovos', 'obitos_novos')
            .drop('casosacumulado', 'populacaotcu2019')
        )

    def dados_gerais(self):
        """
        Visualização 4: casos/obitos/incidencia/mortalidade/atualização por região/brasil
        """
        return (
            self.base_df
            .filter('codmun is null')
            .select('data', 'regiao', 'estado', 'casosacumulado', 'obitosacumulado', 'populacaotcu2019')
            .groupBy('data', 'regiao')
            .agg(
                sum('casosacumulado').alias('casos'),
                sum('obitosacumulado').alias('obitos'),
                sum('populacaotcu2019').alias('populacao')
            )
            .withColumn(
                'incidencia',
                round(col('casos').cast('double') * 100000 / col('populacao').cast('double'), 1)
            )
            .withColumn(
                'mortalidade',
                round(col('obitos').cast('double') * 100000 / col('populacao').cast('double'), 1)
            )
            .drop('populacao')
        )
