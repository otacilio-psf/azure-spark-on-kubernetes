from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
import os
from schema import schema_enade
import unicodedata

class SparkJob:
    storage_name = "datalakeigtibootcamp"
    container_name = "datalake"
    path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/"
    bronze_path = "bronze/enade/MICRODADOS_ENADE_2017.txt"
    silver_path = "silver/enade"
    gold_path = "gold/enade"

    def _spark_session(self):
        print("============================== SparkSession Starting ")
        self.spark = SparkSession.builder.appName("desafio-igti-cde-mod4").getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        print("============================== Set Spark Conf ")
        self.spark.conf.set(f"fs.azure.account.auth.type.{self.storage_name}.dfs.core.windows.net", "OAuth" )
        self.spark.conf.set(f"fs.azure.account.oauth.provider.type.{self.storage_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set(f"fs.azure.account.oauth2.client.id.{self.storage_name}.dfs.core.windows.net", os.getenv('AZURE_CLIENT_ID'))
        self.spark.conf.set(f"fs.azure.account.oauth2.client.secret.{self.storage_name}.dfs.core.windows.net", os.getenv('AZURE_CLIENT_SECRET'))
        self.spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{self.storage_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{os.getenv('AZURE_TENANT_ID')}/oauth2/token")

    def _read_bronze_data(self):
        print("============================== Read bronze data ")
        self.df_bronze = (self.spark.read.format("csv")
                             .option("header", "true")
                             .option("sep", ";")
                             .schema(schema_enade)
                             .load(self.path+self.bronze_path))
        self.df_bronze.printSchema()

    def _transform_bronze_data(self):
        print("============================== Transform bronze data ")
        double_col = ['NT_GER', 'NT_FG', 'NT_OBJ_FG', 'NT_DIS_FG', 'NT_CE', 'NT_OBJ_CE', 'NT_DIS_CE']
        def double_converter(col_list):
            def _(df):
                for col_ in col_list:
                    df = (df.withColumn(col_, sf.regexp_replace(sf.col(col_), '\\.', ''))
                            .withColumn(col_, sf.regexp_replace(sf.col(col_), ',', '.'))
                            .withColumn(col_, sf.col(col_).cast("double")))
                return df
            return _

        self.df_bronze = (self.df_bronze.transform(double_converter(double_col))
                                        .withColumn('ANO', sf.lit(2017))
                                        .withColumn('CREATE_TIMESTAMP', sf.current_timestamp()))

        self.df_bronze.select('NT_GER').printSchema()
        self.df_bronze.select('NT_GER').show()

    def _write_silver_data(self):
        print("============================== Write silver data ")
        (self.df_bronze.write.mode("overwrite")
                             .format("delta")
                             .partitionBy('ANO')
                             .save(self.path+self.silver_path))
    
    def _read_silver_data(self):
        print("============================== Read silver data ")
        self.df_silver = (self.spark.read.format("delta")
                             .load(self.path+self.silver_path))
        self.df_municipio = (self.spark.read.format("csv")
                             .option("header", "true")
                             .option('encoding', 'ISO-8859-1')
                             .schema("CO_MUNIC_CURSO INT, CO_MUNIC_CURSO_DESC STRING, UF_SIGLA STRING")
                             .load("dimensao_municipio.csv"))    
        self.df_cursos = (self.spark.read.format("csv")
                             .option("header", "true")
                             .option('encoding', 'ISO-8859-1')
                             .schema("CO_GRUPO INT, CO_GRUPO_DESC STRING")
                             .load("dimensao_cursos.csv"))           
        self.df_municipio.printSchema()
        self.df_cursos.printSchema()

    def _transform_silver_data(self):
        print("============================== Transform silver data ")

        @sf.udf
        def unicode_udf(string):
            if string:
                normalized = unicodedata.normalize('NFD', string)
                return normalized.encode('ascii', 'ignore').decode('utf8')
            else:
                return None
        
        self.df_municipio = self.df_municipio.withColumn('CO_MUNIC_CURSO_DESC', unicode_udf(sf.col('CO_MUNIC_CURSO_DESC')))
        self.df_cursos = self.df_cursos.withColumn('CO_GRUPO_DESC', unicode_udf(sf.col('CO_GRUPO_DESC')))

        self.df_silver = (self.df_silver
                              .join(sf.broadcast(self.df_municipio), 'CO_MUNIC_CURSO', 'left') # map municipio
                              .join(sf.broadcast(self.df_cursos), 'CO_GRUPO', 'left') # map cursos
                              )

    def _write_gold_data(self):
        print("============================== Write gold data ")
        (self.df_silver.write.mode("overwrite")
                             .format("delta")
                             .partitionBy('ANO', 'UF_SIGLA')
                             .save(self.path+self.gold_path))

    def process(self):
        self._spark_session()
        self._read_bronze_data()
        self._transform_bronze_data()
        self._write_silver_data()
        self._read_silver_data()
        self._transform_silver_data()
        self._write_gold_data()
        print("============================== Finished ")
        self.spark.stop()

if __name__ == "__main__":
    sj = SparkJob()
    sj.process()
