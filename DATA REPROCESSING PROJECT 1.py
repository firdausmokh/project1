# -*- coding: utf-8 -*-
"""
Created on Thu May  2 14:50:30 2024

@author: Firdaus Mokhtar
"""

import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
from urllib.parse import quote
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Creating engine which connect to MySQL
user = 'root' # user name
pw = 'Dipot1996@' # password
db = 'project_1' # database

# creating engine to connect database
engine = create_engine(f"mysql+pymysql://{user}:%s@localhost/{db}" %quote(f'{pw}'))

data = pd.read_csv(r"C:\Users\Firdaus Mokhtar\OneDrive\360TMG\Project 1\Machine Downtime.csv")
data.info()

# dumping data into database 
data.to_sql('machine_downtime', con = engine, if_exists = 'replace', chunksize = 1000, index = False)

# Data Ingestion from secondary source
# loading data from database
sql = 'select * from machine_downtime'

data1 = pd.read_sql_query(sql, con = engine)
print(data1)

### Break the table into 2 main column ######

machine_failure = data1[(data1['Downtime'] == 'Machine_Failure')]
machine_failure.info

no_machine_failure = data1[(data1)['Downtime'] == 'No_Machine_Failure']
no_machine_failure.info

######################### Data Reprocessing #########################################


######## 1. Type casting #####################

# Check all the data type for both table
machine_failure.dtypes
no_machine_failure.dtypes

# There is no change as the data types are suitables for each column.

######### 2. Duplicate values ##############

duplicate_fail = machine_failure.duplicated() #return boolean series denoting duplicate rows
duplicate_fail #show the result
sum(duplicate_fail) #check the sum of duplicate value

duplicate_nofail = no_machine_failure.duplicated() #same procedure but different data
duplicate_nofail
sum(duplicate_nofail)

# Conclusion : No duplicate value

######## 3. Outlier analysis treatment ############

# Find the outlier from each column

# Machine failure
sns.boxplot(machine_failure['Hydraulic_Pressure(bar)']) #have outlier
sns.boxplot(machine_failure['Coolant_Pressure(bar)']) #have outlier
sns.boxplot(machine_failure['Air_System_Pressure(bar)']) #have outlier
sns.boxplot(machine_failure['Coolant_Temperature']) # no outlier
sns.boxplot(machine_failure['Hydraulic_Oil_Temperature(°C)']) #have outlier
sns.boxplot(machine_failure['Spindle_Bearing_Temperature(°C)']) #have outlier
sns.boxplot(machine_failure['Spindle_Vibration(µm)']) #have outlier
sns.boxplot(machine_failure['Tool_Vibration(µm)']) #have outlier
sns.boxplot(machine_failure['Spindle_Speed(RPM)']) #have outlier
sns.boxplot(machine_failure['Voltage(volts)']) #have outlier
sns.boxplot(machine_failure['Torque(Nm)']) #have outlier
sns.boxplot(machine_failure['Cutting(kN)']) #no outlier

# Detection of outliers for Coolant Pressure & Torque

#Coolant Pressure 
IQR = machine_failure['Coolant_Pressure(bar)'].quantile(0.75) - machine_failure['Coolant_Pressure(bar)'].quantile(0.25)
IQR #1.0218241410000006

lower_limit_cf = machine_failure['Coolant_Pressure(bar)'].quantile(0.25) - (IQR * 1.5)
lower_limit_cf #3.0132962204999987

upper_limit_cf = machine_failure['Coolant_Pressure(bar)'].quantile(0.75) + (IQR * 1.5)
upper_limit_cf #7.100592784500002

#Torque
IQR_1 = machine_failure['Torque(Nm)'].quantile(0.75) - machine_failure['Torque(Nm)'].quantile(0.25)
IQR_1 #9.359703250000003

lower_limit_tf = machine_failure['Torque(Nm)'].quantile(0.25) - (IQR * 1.5)
lower_limit_tf #15.431368858499997

upper_limit_tf = machine_failure['Torque(Nm)'].quantile(0.75) + (IQR * 1.5)
upper_limit_tf #27.856544531500003

#Trim the outlier

#Coolant Pressure
#Flag the outliers
outliers_cf = np.where(machine_failure['Coolant_Pressure(bar)'] > upper_limit_cf, True, np.where(machine_failure['Coolant_Pressure(bar)'] < lower_limit_cf, True, False))

#Outliers data
cf_out = machine_failure.loc[outliers_cf, ] #sort out the outliers

cf_trimmed = machine_failure.loc[~(outliers_cf), ] #trim the outliers
machine_failure.shape, cf_trimmed.shape #check the data frame

#Check the trimmed dataset
sns.boxplot(cf_trimmed['Coolant_Pressure(bar)']) #no outliers

#Torque
#Flag the outliers
outliers_tf = np.where(machine_failure['Torque(Nm)'] > upper_limit_tf, True, np.where(machine_failure['Torque(Nm)'] < lower_limit_tf, True, False))

#Outliers data
tf_out = machine_failure.loc[outliers_tf, ] #sort out the outliers

tf_trimmed = machine_failure.loc[~(outliers_tf), ] #trim the outliers
machine_failure.shape, tf_trimmed.shape #check the data frame

#Check the trimmed dataset
sns.boxplot(tf_trimmed['Torque(Nm)']) #no outliers

# No Machine Failure
sns.boxplot(no_machine_failure['Hydraulic_Pressure(bar)']) #have outlier
sns.boxplot(no_machine_failure['Coolant_Pressure(bar)']) #have outlier
sns.boxplot(no_machine_failure['Air_System_Pressure(bar)']) #have outlier
sns.boxplot(no_machine_failure['Coolant_Temperature']) #have outlier
sns.boxplot(no_machine_failure['Hydraulic_Oil_Temperature(°C)']) #have outlier
sns.boxplot(no_machine_failure['Spindle_Bearing_Temperature(°C)']) #have outlier
sns.boxplot(no_machine_failure['Spindle_Vibration(µm)']) #have outlier
sns.boxplot(no_machine_failure['Tool_Vibration(µm)']) #have outlier
sns.boxplot(no_machine_failure['Spindle_Speed(RPM)']) #no outlier
sns.boxplot(no_machine_failure['Voltage(volts)']) #have outlier
sns.boxplot(no_machine_failure['Torque(Nm)']) #have outlier
sns.boxplot(no_machine_failure['Cutting(kN)']) #no outlier

# Detection of outliers for Hydraulic Pressure, Coolant Pressure & Torque

#Coolant Pressure 
IQR_4 = no_machine_failure['Hydraulic_Pressure(bar)'].quantile(0.75) - no_machine_failure['Hydraulic_Pressure(bar)'].quantile(0.25)
IQR_4 #34.905623225000014

lower_limit_pnf = no_machine_failure['Hydraulic_Pressure(bar)'].quantile(0.25) - (IQR * 1.5)
lower_limit_pnf #99.7817647885

upper_limit_pnf = no_machine_failure['Hydraulic_Pressure(bar)'].quantile(0.75) + (IQR * 1.5)
upper_limit_pnf #137.75286043650001

#Coolant Pressure 
IQR_2 = no_machine_failure['Coolant_Pressure(bar)'].quantile(0.75) - no_machine_failure['Coolant_Pressure(bar)'].quantile(0.25)
IQR_2 #1.5856947562500001

lower_limit_cnf = no_machine_failure['Coolant_Pressure(bar)'].quantile(0.25) - (IQR * 1.5)
lower_limit_cnf #2.3560984814999992

upper_limit_cnf = no_machine_failure['Coolant_Pressure(bar)'].quantile(0.75) + (IQR * 1.5)
upper_limit_cnf #7.007265660750001

#Torque
IQR_3 = no_machine_failure['Torque(Nm)'].quantile(0.75) - no_machine_failure['Torque(Nm)'].quantile(0.25)
IQR_3 #11.692252724999996

lower_limit_tnf = no_machine_failure['Torque(Nm)'].quantile(0.25) - (IQR * 1.5)
lower_limit_tnf #11.692252724999996

upper_limit_tnf = no_machine_failure['Torque(Nm)'].quantile(0.75) + (IQR * 1.5)
upper_limit_tnf #11.692252724999996

#Trim the outlier

#Hydraulic Pressure
#Flag the outliers
outliers_pnf = np.where(no_machine_failure['Hydraulic_Pressure(bar)'] > upper_limit_pnf, True, np.where(no_machine_failure['Hydraulic_Pressure(bar)'] < lower_limit_pnf, True, False))

#Outliers data
pnf_out = no_machine_failure.loc[outliers_pnf, ] #sort out the outliers

pnf_trimmed = no_machine_failure.loc[~(outliers_pnf), ] #trim the outliers
machine_failure.shape, pnf_trimmed.shape #check the data frame

#Check the trimmed dataset
sns.boxplot(pnf_trimmed['Hydraulic_Pressure(bar)']) #no outliers

#Coolant Pressure
#Flag the outliers
outliers_cnf = np.where(no_machine_failure['Coolant_Pressure(bar)'] > upper_limit_cnf, True, np.where(no_machine_failure['Coolant_Pressure(bar)'] < lower_limit_cnf, True, False))

#Outliers data
cnf_out = no_machine_failure.loc[outliers_cnf, ] #sort out the outliers

cnf_trimmed = no_machine_failure.loc[~(outliers_cnf), ] #trim the outliers
machine_failure.shape, cnf_trimmed.shape #check the data frame

#Check the trimmed dataset
sns.boxplot(cnf_trimmed['Coolant_Pressure(bar)']) #no outliers

#Torque
#Flag the outliers
outliers_tnf = np.where(no_machine_failure['Torque(Nm)'] > upper_limit_tnf, True, np.where(no_machine_failure['Torque(Nm)'] < lower_limit_tnf, True, False))

#Outliers data
tnf_out = no_machine_failure.loc[outliers_tnf, ] #sort out the outliers

tnf_trimmed = no_machine_failure.loc[~(outliers_tnf), ] #trim the outliers
machine_failure.shape, tnf_trimmed.shape #check the data frame

#Check the trimmed dataset
sns.boxplot(tnf_trimmed['Torque(Nm)']) #no outliers


######### 4. Zero & Near Zero Variance Features ##############

#Check whether Machine_ID is correlated toward machine failure and no failure downtime
machine_failure.Machine_ID.value_counts()
no_machine_failure.Machine_ID.value_counts()
data.Machine_ID.value_counts()

#It shows that all machine ID are fail and non fail in nearest quantity, as we can conclude machine ID is not relevant toward our analysis

#Drop Date, Machine_ID, Assembly_Line_No and Downtime as the column not important in our analysis
machine_failure.drop(columns = ['Date','Machine_ID','Assembly_Line_No', 'Downtime'], inplace = True)
no_machine_failure.drop(columns = ['Date','Machine_ID','Assembly_Line_No', 'Downtime'], inplace = True)

#Check the variance for both numeric value table machine failure and no machine failure
machine_failure.var()
no_machine_failure.var() 

#Check the whether there are any column that produce zero variance
machine_failure.var() == 0
no_machine_failure.var() == 0

#There are no column that produced zero variance but there are several variance that near to zero but we will not drop yet to check the credibility of the data


###### 5. Missing Values #########

#check for count of NA's in column
machine_failure.isna().sum() 
no_machine_failure.isna().sum()

# As the missing value for the machine failure tables is less than 3% from the overall data, hence it will not effect our analysis
machine_failure.dropna(inplace = True)
machine_failure.isna().sum() #check again the data to see the missing value had been drop

# As the missing value for the no machine failure tables is less than 9% from the overall data, hence it will not effect our analysis. 
no_machine_failure.dropna(inplace = True)
no_machine_failure.isna().sum() #check again the data to see the missing value had been drop

# Conclusion : All the missing value row had been drop to ensure our credibilty of data and also as the overall missing value data is less than 10% from overall data, it will no effected toward our analysis

##### 6. Transformation ########
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# 6a. Check for normal distribution

# Specifiy the column
hydraulic_pressure_fail = machine_failure['Hydraulic_Pressure(bar)']
hydraulic_pressure_no_fail = no_machine_failure['Hydraulic_Pressure(bar)']

coolant_pressure_fail = machine_failure['Coolant_Pressure(bar)']
coolant_pressure_no_fail = no_machine_failure['Coolant_Pressure(bar)']

air_system_pressure_fail = machine_failure['Air_System_Pressure(bar)']
air_system_pressure_no_fail = no_machine_failure['Air_System_Pressure(bar)']

coolant_temperature_fail = machine_failure['Coolant_Temperature']
coolant_temperature_no_fail = no_machine_failure['Coolant_Temperature']

hydraulic_oil_temperature_fail = machine_failure['Hydraulic_Oil_Temperature(°C)']
hydraulic_oil_temperature_no_fail  = no_machine_failure['Hydraulic_Oil_Temperature(°C)']

spindle_bearing_temperature_fail = machine_failure['Spindle_Bearing_Temperature(°C)']
spindle_bearing_temperature_no_fail = no_machine_failure['Spindle_Bearing_Temperature(°C)']

spindle_vibration_fail = machine_failure['Spindle_Vibration(µm)']
spindle_vibration_no_fail = no_machine_failure['Spindle_Vibration(µm)']

tool_vibration_fail = machine_failure['Tool_Vibration(µm)']
tool_vibration_no_fail = no_machine_failure['Tool_Vibration(µm)']

spindle_speed_fail = machine_failure['Spindle_Speed(RPM)']
spindle_speed_no_fail = no_machine_failure['Spindle_Speed(RPM)']

voltage_fail = machine_failure['Voltage(volts)']
voltage_no_fail = no_machine_failure['Voltage(volts)']

torque_fail = machine_failure['Torque(Nm)']
torque_no_fail = no_machine_failure['Torque(Nm)']

cutting_fail = machine_failure['Cutting(kN)']
cutting_no_fail = no_machine_failure['Cutting(kN)']


# Create probability distribution plots, run both code together to see the overlapping distribution
sns.histplot(hydraulic_pressure_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(hydraulic_pressure_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(coolant_pressure_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(coolant_pressure_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(air_system_pressure_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(air_system_pressure_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(coolant_temperature_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(coolant_temperature_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(hydraulic_oil_temperature_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(hydraulic_oil_temperature_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(spindle_bearing_temperature_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(spindle_bearing_temperature_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(spindle_vibration_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(spindle_vibration_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(tool_vibration_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(tool_vibration_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(spindle_speed_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(spindle_speed_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(voltage_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(voltage_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(torque_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(torque_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)

sns.histplot(cutting_fail, kde=True, color='blue', label= 'Fail', alpha=0.5)
sns.histplot(cutting_no_fail, kde=True, color='orange', label='No Fail', alpha=0.5)















