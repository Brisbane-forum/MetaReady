import xlsxwriter,configparser
##########################################################
# File name: run.py
# Author: Paul Fry
# Date created: 30/08/2017
# Date last modified: N/A
# Python Version: 3.6
##########################################################
# Read input from config file
##########################################################
config = configparser.ConfigParser()
config.read("config.ini")

data_src = config['DEFAULT']['data_src']

# 1. Create a workbook and add a worksheet_data_process.
workbook = xlsxwriter.Workbook('mysql-generator.xlsx')
worksheet_data_process = workbook.add_worksheet('DATA_PROCESS')
bold = workbook.add_format({'bold': 1})

# 2. Write the data headers.
worksheet_data_process.write('A1', 'ID', bold)
worksheet_data_process.write('B1', 'AWS_ID', bold)
worksheet_data_process.write('C1', 'PROCESS_NAME', bold)
worksheet_data_process.write('D1', 'PROCESS_DESCRIPTION', bold)
worksheet_data_process.write('E1', 'DATA_PACKAGE_TYPE_ID', bold)
worksheet_data_process.write('F1', 'PROCESS_TYPE', bold)
worksheet_data_process.write('G1', 'TABLE_NAME', bold)
worksheet_data_process.write('H1', 'PARAMETERS', bold)
worksheet_data_process.write('I1', 'SQL', bold)

# 3. Start from the first cell. Rows and columns are zero indexed.
row = 1
col = 0

# 4. initialise/generate field values
ID=0
process_name='process%sDataLoad' % data_src
data_package_type_id=0
process_type='SNS'

iplist = os.listdir('ip/csv')
os.chdir('ip/csv')
for ip_file in iplist:
	ID += 1
	data_package_type_id += 1
	#remove the .csv extn
	table_name=os.path.splitext(ip_file)[0]
	# replace '-'s from table_name
	table_name = table_name.replace('-','_')
	# remove #'s from table_name
	table_name = ''.join([i for i in table_name if not i.isdigit()])
	# remove the final character from the var (it's always an underscore)
	table_name = table_name[:-1]
	table_name=table_name.split('_')[1]
	
	process_description='The Lambda process to load %s data into Redshift.' % table_name
	
	with open(ip_file, encoding='utf-8-sig') as schema:
		#lift & strip out the parameters from the ipfile
		parameters = schema.readline()
		parameters = parameters[:-1]
		parameters = "(%s)" % parameters

	sql="INSERT INTO DATA_PROCESS VALUES (%s, ' ', '%s','%s','%s','%s','landing.%s_%s %s');" % (ID, process_name,  process_description, data_package_type_id, process_type, data_src.lower(), table_name, parameters)

	dataset = (
		[ID, ' ', process_name, process_description, data_package_type_id, process_type, table_name, parameters, sql],
	)

	for ID, AWS_ID, process_name, process_description, data_package_type_id, process_type, table_name, parameters, sql in (dataset):
		worksheet_data_process.write(row, col,     ID)
		worksheet_data_process.write(row, col + 1, AWS_ID)
		worksheet_data_process.write(row, col + 2, process_name)
		worksheet_data_process.write(row, col + 3, process_description)
		worksheet_data_process.write(row, col + 4, data_package_type_id)
		worksheet_data_process.write(row, col + 5, process_type)
		worksheet_data_process.write(row, col + 6, table_name)
		worksheet_data_process.write(row, col + 7, parameters)
		worksheet_data_process.write(row, col + 8, sql)
		row += 1
	
os.chdir('../../')		

###########################################################################################
# sheet 2 - DATA_OBJECT_TYPE
###########################################################################################
# 1. add a worksheet DATA_OBJECT_TYPE
worksheet_DATA_OBJECT_TYPE = workbook.add_worksheet('DATA_OBJECT_TYPE')

# 2. Write the data headers.
worksheet_DATA_OBJECT_TYPE.write('A1', 'ID', bold)
worksheet_DATA_OBJECT_TYPE.write('B1', 'DATA_PACKAGE_TYPE_ID', bold)
worksheet_DATA_OBJECT_TYPE.write('C1', 'DATA_OBJECT_TYPE_NAME', bold)
worksheet_DATA_OBJECT_TYPE.write('D1', 'DATA_OBJECT_DESCRIPTION', bold)
worksheet_DATA_OBJECT_TYPE.write('E1', 'CODE', bold)
worksheet_DATA_OBJECT_TYPE.write('F1', 'S3_LANDING_LOCATION', bold)
worksheet_DATA_OBJECT_TYPE.write('G1', 'NAMING_CONVENTION', bold)
worksheet_DATA_OBJECT_TYPE.write('H1', 'DEFAULT_CLASSIFICATION', bold)
worksheet_DATA_OBJECT_TYPE.write('I1', 'DATA_PIPELINE_ID', bold)
worksheet_DATA_OBJECT_TYPE.write('J1', 'METADATA_TAGS', bold)
worksheet_DATA_OBJECT_TYPE.write('K1', 'SQL', bold)

# 3. Start from the first cell. Rows and columns are zero indexed.
row = 1
col = 0

# 4. initialise/generate field values
ID=0
DATA_PACKAGE_TYPE_ID=0
DATA_OBJECT_TYPE_NAME='DATA FILES'
DATA_OBJECT_DESCRIPTION='Data Files'
CODE='NULL'
S3_LANDING_LOCATION='aws298-ingestion:/objects/'
DEFAULT_CLASSIFICATION='CLASSIFIED'
DATA_PIPELINE_ID=0
METADATA_TAGS='NULL'

# 5. Loop through each of the input files to fetch the table names
iplist = os.listdir('ip/csv')
os.chdir('ip/csv')
for ip_file in iplist:
	ID += 1
	DATA_PACKAGE_TYPE_ID += 1
	DATA_PIPELINE_ID += 1
	#remove the .csv extn
	table_name=os.path.splitext(ip_file)[0]
	# replace '-'s from table_name
	table_name = table_name.replace('-','_')
	# remove #'s from table_name
	table_name = ''.join([i for i in table_name if not i.isdigit()])
	# remove the final character from the var (it's always an underscore)
	table_name = table_name[:-1]
	table_name=table_name.split('_')[1]
	
	NAMING_CONVENTION='.*%s.*' % table_name
	SQL="INSERT INTO DATA_OBJECT_TYPE VALUES (%s,%s,'%s','%s','%s','%s','%s','%s',%s,'%s');" % (ID,DATA_PACKAGE_TYPE_ID,DATA_OBJECT_TYPE_NAME,DATA_OBJECT_DESCRIPTION,CODE,S3_LANDING_LOCATION,NAMING_CONVENTION,DEFAULT_CLASSIFICATION,DATA_PIPELINE_ID,METADATA_TAGS)

	dataset = (
		[ID,DATA_PACKAGE_TYPE_ID,DATA_OBJECT_TYPE_NAME,DATA_OBJECT_DESCRIPTION,CODE,S3_LANDING_LOCATION,NAMING_CONVENTION,DEFAULT_CLASSIFICATION,DATA_PIPELINE_ID,METADATA_TAGS,SQL],
	)

	for ID,DATA_PACKAGE_TYPE_ID,DATA_OBJECT_TYPE_NAME,DATA_OBJECT_DESCRIPTION,CODE,S3_LANDING_LOCATION,NAMING_CONVENTION,DEFAULT_CLASSIFICATION,DATA_PIPELINE_ID,METADATA_TAGS,SQL in (dataset):
		worksheet_DATA_OBJECT_TYPE.write(row, col, ID)
		worksheet_DATA_OBJECT_TYPE.write(row, col + 1, DATA_PACKAGE_TYPE_ID)
		worksheet_DATA_OBJECT_TYPE.write(row, col + 2, DATA_OBJECT_TYPE_NAME)
		worksheet_DATA_OBJECT_TYPE.write(row, col + 3, DATA_OBJECT_DESCRIPTION)
		worksheet_DATA_OBJECT_TYPE.write(row, col + 4, CODE)
		worksheet_DATA_OBJECT_TYPE.write(row, col + 5, S3_LANDING_LOCATION)
		worksheet_DATA_OBJECT_TYPE.write(row, col + 6, NAMING_CONVENTION)
		worksheet_DATA_OBJECT_TYPE.write(row, col + 7, DEFAULT_CLASSIFICATION)
		worksheet_DATA_OBJECT_TYPE.write(row, col + 8, DATA_PIPELINE_ID)
		worksheet_DATA_OBJECT_TYPE.write(row, col + 9, METADATA_TAGS)
		worksheet_DATA_OBJECT_TYPE.write(row, col + 10, SQL)
		row += 1

os.chdir('../../')

###########################################################################################
# sheet 3 - DATA_PACKAGE_TYPE
###########################################################################################
worksheet_DATA_PACKAGE_TYPE = workbook.add_worksheet('DATA_PACKAGE_TYPE')

# 2. Write the data headers.
worksheet_DATA_PACKAGE_TYPE.write('A1', 'ID', bold)
worksheet_DATA_PACKAGE_TYPE.write('B1', 'DATA_PACKAGE_NAME', bold)
worksheet_DATA_PACKAGE_TYPE.write('C1', 'DATA_PACKAGE_DESCRIPTION', bold)
worksheet_DATA_PACKAGE_TYPE.write('D1', 'CODE', bold)
worksheet_DATA_PACKAGE_TYPE.write('E1', 'S3_REGEX_MATCH', bold)
worksheet_DATA_PACKAGE_TYPE.write('F1', 'FREQUENCY', bold)
worksheet_DATA_PACKAGE_TYPE.write('G1', 'DELIVERY_TYPE', bold)
worksheet_DATA_PACKAGE_TYPE.write('H1', 'METADATA_TAGS', bold)
worksheet_DATA_PACKAGE_TYPE.write('I1', 'SOURCE_SYSTEM_ID', bold)
worksheet_DATA_PACKAGE_TYPE.write('J1', 'SQL', bold)

# 3. Start from the first cell. Rows and columns are zero indexed.
row = 1
col = 0

# 4. initialise/generate field values
ID=0
CODE=data_src.upper()
FREQUENCY="ADHOC"
DELIVERY_TYPE="File Push"
METADATA_TAGS="NULL"
SOURCE_SYSTEM_ID=2

# 5. Loop through each of the input files to fetch the table names
iplist = os.listdir('ip/csv')
os.chdir('ip/csv')
for ip_file in iplist:
	ID += 1
	#remove the .csv extn
	table_name=os.path.splitext(ip_file)[0]
	# replace '-'s from table_name
	table_name = table_name.replace('-','_')
	# remove #'s from table_name
	table_name = ''.join([i for i in table_name if not i.isdigit()])
	# remove the final character from the var (it's always an underscore)
	table_name = table_name[:-1]
	table_name=table_name.split('_')[1]
	
	DATA_PACKAGE_NAME='%s %s' % (data_src, table_name)
	DATA_PACKAGE_DESCRIPTION='%s %s Extract' % (data_src, table_name)
	S3_REGEX_MATCH='aws298-ingestion:/packages/.*%s.*' % table_name
	SQL="INSERT INTO DATA_PACKAGE_TYPE VALUES (%s,'%s','%s','%s','%s','%s','%s','%s',%s);" % (ID,DATA_PACKAGE_NAME,DATA_PACKAGE_DESCRIPTION,CODE,S3_REGEX_MATCH,FREQUENCY,DELIVERY_TYPE,METADATA_TAGS,SOURCE_SYSTEM_ID)

	dataset = (
		[ID,DATA_PACKAGE_NAME,DATA_PACKAGE_DESCRIPTION,CODE,S3_REGEX_MATCH,FREQUENCY,DELIVERY_TYPE,METADATA_TAGS,SOURCE_SYSTEM_ID,SQL],
	)
	
	for ID,DATA_PACKAGE_NAME,DATA_PACKAGE_DESCRIPTION,CODE,S3_REGEX_MATCH,FREQUENCY,DELIVERY_TYPE,METADATA_TAGS,SOURCE_SYSTEM_ID, SQL in (dataset):
		worksheet_DATA_PACKAGE_TYPE.write(row, col, ID)
		worksheet_DATA_PACKAGE_TYPE.write(row, col + 1, DATA_PACKAGE_NAME)
		worksheet_DATA_PACKAGE_TYPE.write(row, col + 2, DATA_PACKAGE_DESCRIPTION)
		worksheet_DATA_PACKAGE_TYPE.write(row, col + 3, CODE)
		worksheet_DATA_PACKAGE_TYPE.write(row, col + 4, S3_REGEX_MATCH)
		worksheet_DATA_PACKAGE_TYPE.write(row, col + 5, FREQUENCY)
		worksheet_DATA_PACKAGE_TYPE.write(row, col + 6, DELIVERY_TYPE)
		worksheet_DATA_PACKAGE_TYPE.write(row, col + 7, METADATA_TAGS)
		worksheet_DATA_PACKAGE_TYPE.write(row, col + 8, SOURCE_SYSTEM_ID)
		worksheet_DATA_PACKAGE_TYPE.write(row, col + 9, SQL)
		row += 1

os.chdir('../../')	
workbook.close()