from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
import yaml

class ReadInputFile:
    def __init__(self, spark): 
        '''initialize spark'''
        self.spark = spark

    def fileread(self, config: dict, filename: str) -> DataFrame:
        '''Reading required file for dataframe'''
        df = self.spark.read.csv(config[filename], header=True)
        return df

    def conf(self, config_path: str) -> dict:
        '''initialize config file'''
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            return config
        
    def analytics1(self, df_Primary_Person_use: DataFrame):
        '''Analysis 1: Find the number of crashes (accidents) in which number of males killed are greater than 2'''
        male_deaths_df = df_Primary_Person_use.filter(
        (df_Primary_Person_use.PRSN_GNDR_ID == 'MALE') & 
        (df_Primary_Person_use.PRSN_INJRY_SEV_ID == 'KILLED')
         ).withColumn("DEATH_CNT", F.col("DEATH_CNT").cast("integer")).groupBy('CRASH_ID').agg(F.sum('DEATH_CNT').alias('DEATH_CNT'))
        male_deaths_final = male_deaths_df.filter(F.col('DEATH_CNT') > 2).count()
        print(male_deaths_final)
    
    
    def analytics2(self, df_Units_use: DataFrame):
        '''Analysis 2: How many two wheelers are booked for crashes? '''
        two_wheelers_df = df_Units_use.filter(df_Units_use.VEH_BODY_STYL_ID.isin(['MOTORCYCLE', 'POLICE MOTORCYCLE']))
        two_wheeler_count = two_wheelers_df.select('CRASH_ID', 'UNIT_NBR').distinct().count()
        print(two_wheeler_count)
        
    def analytics3(self, df_Primary_Person_use: DataFrame, df_Units_use: DataFrame):
        '''Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy'''
        df_person = df_Primary_Person_use.\
        filter((df_Primary_Person_use.PRSN_TYPE_ID =='DRIVER') & (df_Primary_Person_use.PRSN_INJRY_SEV_ID == 'KILLED') &(df_Primary_Person_use.PRSN_AIRBAG_ID == 'NOT DEPLOYED')).select('CRASH_ID', 'UNIT_NBR', 'PRSN_TYPE_ID')

        join_condition = (df_person.CRASH_ID == df_Units_use.CRASH_ID) & \
                 (df_person.UNIT_NBR == df_Units_use.UNIT_NBR)

        df_joined = df_person.join(df_Units_use, on=join_condition, how='inner')
        df_vehicle=df_joined.filter(df_Units_use.VEH_BODY_STYL_ID.\
        isin(['PASSENGER CAR, 4-DOOR', 'PASSENGER CAR, 2-DOOR','SPORT UTILITY VEHICLE','VAN','AMBULANCE','POLICE CAR/TRUCK'])).groupBy('VEH_MAKE_ID').count().orderBy('count', ascending=False).limit(5)

        df_vehicle.show()

    def analytics4(self, df_Primary_Person_use: DataFrame, df_Units_use: DataFrame):
        '''Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? '''
        df_person_alias = df_Primary_Person_use.alias('person')
        df_unit_alias = df_Units_use.alias('unit')


        df_person = df_person_alias.filter(
        df_person_alias.DRVR_LIC_TYPE_ID.isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'])
        ).select('person.CRASH_ID', 'person.UNIT_NBR')

        df_unit = df_unit_alias.filter(
        df_unit_alias.VEH_HNR_FL == 'Y'
        ).select('unit.CRASH_ID', 'unit.UNIT_NBR')


        join_condition = (col('person.CRASH_ID') == col('unit.CRASH_ID')) & \
                 (col('person.UNIT_NBR') == col('unit.UNIT_NBR'))


        df_interim = df_person.join(df_unit, on=join_condition, how='inner') \
        .select('person.CRASH_ID', 'person.UNIT_NBR').distinct().count()

        print(df_interim)

    def analytics5(self, df_Primary_Person_use: DataFrame):
        '''Analysis 5: Which state has highest number of accidents in which females are not involved? '''
        df_person = df_Primary_Person_use.filter(df_Primary_Person_use.PRSN_GNDR_ID != 'FEMALE') \
        .groupBy('DRVR_LIC_STATE_ID') \
        .agg(F.count('CRASH_ID').alias('count')) \
        .orderBy('count', ascending=False).limit(1).select('DRVR_LIC_STATE_ID')
        df_person.show()
    

    def analytics6(self, df_Primary_Person_use: DataFrame,df_Units_use: DataFrame):
        '''Analysis 6:Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death'''
        df_person=df_Primary_Person_use.filter(df_Primary_Person_use.PRSN_INJRY_SEV_ID.isin(['KILLED','INCAPACITATING INJURY','NON-INCAPACITATING INJURY','POSSIBLE INJURY'])).select('CRASH_ID', 'UNIT_NBR')
        df_person_alias = df_person.alias('person')
        df_units_use_alias = df_Units_use.alias('units')

        join_condition = (df_person_alias['CRASH_ID'] == df_units_use_alias['CRASH_ID']) & \
                 (df_person_alias['UNIT_NBR'] == df_units_use_alias['UNIT_NBR'])

        df_unit = df_person_alias.join(df_units_use_alias, on=join_condition, how='inner') \
        .groupBy('VEH_MAKE_ID').agg(F.count('person.CRASH_ID').alias('crash_count'))

        windowSpec  = Window.orderBy(F.desc("crash_count"))
        df_unit=df_unit.withColumn("rank", F.dense_rank().over(windowSpec))
        df_unit=df_unit.filter((df_unit.rank >=3) & (df_unit.rank <=5)).orderBy('rank').select('VEH_MAKE_ID').show()

    def analytics7(self, df_Primary_Person_use: DataFrame, df_Units_use: DataFrame):
        '''Analysis 7:For all the body styles involved in crashes, mention 
        the top ethnic user group of each unique body style  '''
        df_Units_use=df_Units_use.distinct()
        df_Primary_Person_use=df_Primary_Person_use.distinct()
        df_joined=df_Primary_Person_use.join(df_Units_use, on=['CRASH_ID','UNIT_NBR'], how='inner').select('CRASH_ID','UNIT_NBR','PRSN_ETHNICITY_ID','VEH_BODY_STYL_ID').groupBy('PRSN_ETHNICITY_ID','VEH_BODY_STYL_ID').agg(F.count('CRASH_ID').alias('count'))
        windowSpec  = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(F.desc("count"))
        df_joined=df_joined.withColumn("rank", F.rank().over(windowSpec))
        df_joined=df_joined.filter(df_joined.rank == 1).select('PRSN_ETHNICITY_ID','VEH_BODY_STYL_ID').show()

    def analytics8(self, df_Primary_Person_use: DataFrame, df_Units_use: DataFrame):
        '''Analysis 8:Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)'''
        df_person=df_Primary_Person_use.filter(df_Primary_Person_use.PRSN_ALC_RSLT_ID =='Positive').select('CRASH_ID', 'UNIT_NBR','DRVR_ZIP')

        df_unit=df_person.join(df_Units_use, on=['CRASH_ID','UNIT_NBR'], how='inner').select('CRASH_ID', 'UNIT_NBR','DRVR_ZIP','VEH_BODY_STYL_ID')

        df_unit=df_unit.filter(df_Units_use.VEH_BODY_STYL_ID.\
        isin(['PASSENGER CAR, 4-DOOR', 'PASSENGER CAR, 2-DOOR','SPORT UTILITY VEHICLE','VAN','AMBULANCE','POLICE CAR/TRUCK'])).groupBy('DRVR_ZIP').agg(F.count('CRASH_ID').alias('count')).orderBy(F.desc("count")).limit(10)

        windowSpec  = Window.orderBy(F.desc("count"))
        df_unit=df_unit.withColumn("rank", F.dense_rank().over(windowSpec))
        df_unit=df_unit.filter((df_unit.rank >=1) & (df_unit.rank <=5)).orderBy('rank').select('DRVR_ZIP','count').show()

    def analytics9(self, df_Damages_use: DataFrame, df_Units_use: DataFrame):
        '''Analysis 9:Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance'''
        df_unit=df_Units_use.filter(df_Units_use.VEH_DMAG_SCL_1_ID.isin(['DAMAGED 5','DAMAGED 5','DAMAGED 7 HIGHEST']) & df_Units_use.FIN_RESP_TYPE_ID.isin(['PROOF OF LIABILITY INSURANCE','LIABILITY INSURANCE POLICY','INSURANCE BINDER','CERTIFICATE OF SELF-INSURANCE','CERTIFICATE OF DEPOSIT WITH COUNTY JUDGE','CERTIFICATE OF DEPOSIT WITH COMPTROLLER'])).select('CRASH_ID').distinct()

        df_join=df_unit.join(df_Damages_use, on=['CRASH_ID'], how='left').select('CRASH_ID','DAMAGED_PROPERTY')
        df_final=df_join.filter(df_join.DAMAGED_PROPERTY.isNull()).select('CRASH_ID').distinct().count()
        print(df_final)


    def analytics10(self, df_Primary_Person_use: DataFrame, df_Units_use: DataFrame,df_Charges_use: DataFrame):
        '''Analysis 10:Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)'''
        df_topstates_highest_offence=df_Primary_Person_use.filter(~df_Primary_Person_use.DRVR_LIC_STATE_ID.isin(['NA','Other','Unknown'])).groupBy('DRVR_LIC_STATE_ID').agg(F.count('CRASH_ID').alias('count')).orderBy(F.desc("count"))
        windowSpec  = Window.orderBy(F.desc("count"))
        df_topstates_highest_offence=df_topstates_highest_offence.withColumn("rank", F.dense_rank().over(windowSpec))
        df_topstates_highest_offence=df_topstates_highest_offence.filter(df_topstates_highest_offence.rank <= 25).select('DRVR_LIC_STATE_ID')
        df_1=df_topstates_highest_offence.select('DRVR_LIC_STATE_ID').rdd.flatMap(lambda x: x).collect()
        df_crashdetails_topstates_highest_offence_licensed=df_Primary_Person_use.filter(df_Primary_Person_use.DRVR_LIC_STATE_ID.isin(df_1) & (df_Primary_Person_use.DRVR_LIC_TYPE_ID.isin(['DRIVER LICENSE','COMMERCIAL DRIVER LIC.']))).select('CRASH_ID','UNIT_NBR','DRVR_LIC_STATE_ID')

        df_toptenused_vehicle_coulour=df_Units_use.groupBy('VEH_COLOR_ID').agg(F.count('CRASH_ID').alias('count')).orderBy(F.desc("count"))
        windowSpec  = Window.orderBy(F.desc("count"))
        df_toptenused_vehicle_coulour=df_toptenused_vehicle_coulour.withColumn("rank", F.dense_rank().over(windowSpec))
        df_toptenused_vehicle_coulour=df_toptenused_vehicle_coulour.filter(df_toptenused_vehicle_coulour.rank <= 10).select('VEH_COLOR_ID')
        df_2=df_toptenused_vehicle_coulour.select('VEH_COLOR_ID').rdd.flatMap(lambda x: x).collect()
        df_crashdetails_usedTop10colur_makers=df_Units_use.filter((df_Units_use.VEH_COLOR_ID.isin(df_2))).select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID')

        df_joined1=df_crashdetails_topstates_highest_offence_licensed.join(df_crashdetails_usedTop10colur_makers,on=['CRASH_ID','UNIT_NBR'], how='inner').select('CRASH_ID','UNIT_NBR','DRVR_LIC_STATE_ID','VEH_MAKE_ID')

        df_speed_charge=df_Charges_use.filter(df_Charges_use.CHARGE.like('%SPEED%')).select('CRASH_ID','UNIT_NBR').distinct()

        df_joined2=df_joined1.join(df_speed_charge,on=['CRASH_ID','UNIT_NBR'], how='inner').select('CRASH_ID','UNIT_NBR','DRVR_LIC_STATE_ID','VEH_MAKE_ID')

        df_top5VehicleMaker=df_joined2.groupBy('VEH_MAKE_ID').agg(F.count('CRASH_ID').alias('count')).\
        select('VEH_MAKE_ID').orderBy(F.desc("count")).limit(5)
        df_top5VehicleMaker.show()


