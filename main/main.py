import sys
import subprocess

sys.path.append('\Users\40103513\Desktop\BCGProject_Ayankesh')
#append the path of the project folder as required.I have used my local project folder location



subprocess.check_call([sys.executable, "-m", "pip", "install", "PyYAML"])
import yaml


from pyspark.sql import SparkSession
from Functions.newTestFunc import ReadInputFile


if __name__ == "__main__":
    spark = SparkSession.builder.appName('app').getOrCreate()
    config_path = "configs\config.yaml"
    #please change the config path and config yaml file for actual source data path
    print('Initializing Spark')
    reader = ReadInputFile(spark)
    print(reader.conf.__doc__)
    config = reader.conf(config_path)
    #used docstring capability as asked
    print(reader.fileread.__doc__)
    df_Charges_use = reader.fileread(config, 'Charges_use_path')
    df_Primary_Person_use=reader.fileread(config, 'Primary_Person_use_path')
    df_Damages_use=reader.fileread(config, 'Damages_use_path')
    df_Endorse_use=reader.fileread(config, 'Endorse_use_path')
    df_Restrict_use=reader.fileread(config, 'Restrict_use_path')
    df_Units_use=reader.fileread(config, 'Units_use_path')
    print(reader.analytics1.__doc__)
    reader.analytics1(df_Primary_Person_use)
    print(reader.analytics2.__doc__)
    reader.analytics2(df_Units_use)
    print(reader.analytics3.__doc__)
    reader.analytics3(df_Primary_Person_use,df_Units_use)
    print(reader.analytics4.__doc__)
    reader.analytics4(df_Primary_Person_use,df_Units_use)
    print(reader.analytics5.__doc__)
    reader.analytics5(df_Primary_Person_use)
    print(reader.analytics6.__doc__)
    reader.analytics6(df_Primary_Person_use,df_Units_use)
    print(reader.analytics7.__doc__)
    reader.analytics7(df_Primary_Person_use,df_Units_use)
    print(reader.analytics8.__doc__)
    reader.analytics8(df_Primary_Person_use,df_Units_use)
    print(reader.analytics9.__doc__)
    reader.analytics9(df_Damages_use,df_Units_use)
    print(reader.analytics10.__doc__)
    reader.analytics10(df_Primary_Person_use,df_Units_use,df_Charges_use)
    
