def moduleType(moduleJson):
    return{
        '506153734175476c4f62416c57734963.4e1b0fe6aded4b3fa36f39b8862b9004.v1-default-1744': getImportData,
        '506153734175476c4f62416c57734963.1ec722fab6234e26a44ea50c6d726223.v1-default-1730': getSelectColumnDataset,
        '506153734175476c4f62416c57734963.90381e8067c34d9987541db785b7ea37.v1-default-1744': getSQLTranform
    }.get(moduleJson['ModuleId'])(moduleJson)
    
def parseImportDataParameters(moduleParameters):
    d = {}
    for i in moduleParameters:
        name = i.pop('Name')
        d[name] = i
    return d

def docdbModule(moduleParameters):
    docdbConfig = {
        'Endpoint': moduleParameters['Azure DocumentDB Server']['Value'],
        'Masterkey': 'InsertYourKeyHere',
        'Database': moduleParameters['Database ID']['Value'],
        'preferredRegions': 'Central US',
        'connectionMode': 'Gateway',
        'Collection': moduleParameters['Collection ID']['Value'],
        'SamplingRatio': '1.0',
        'schema_samplesize': '1000',
        'query_custom': moduleParameters['SQL Query']['Value'],
    }

    print("""
# Connection
config = """+json.dumps(docdbConfig, indent=4)+"""

# Create DataFrame
dataframe = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**config).load()
""")
    
def getImportData(moduleJson):
    moduleParameters = parseImportDataParameters(moduleJson['ModuleParameters'])
    
    #print(json.dumps(moduleParameters, indent=4))
    return {
        'DocumentDB': docdbModule(moduleParameters)
    }.get(moduleParameters['Please Specify Data Source']['Value'],'')

    
def getSelectColumnDataset(moduleJson):
    moduleParameters = parseImportDataParameters(moduleJson['ModuleParameters'])
    
    import urllib
    columns = urllib.parse.unquote(moduleParameters['Select Columns']['Value'])
    json_columns = json.loads(columns.replace("\\",""))
    cols = json_columns['rules'][0]['columns']
    delim = ''  
    dfSelect = 'dataframe = dataframe.select('
    for c in cols:
        dfSelect += delim + 'col("' + c + '")'
        delim = ', '
    dfSelect += ')'
    print(dfSelect)
    
def getSQLTranform(moduleJson):
    moduleParameters = parseImportDataParameters(moduleJson['ModuleParameters'])
    query = moduleParameters['SQL Query Script']['Value'].replace(";","")
    print("""
dataframe.registerTempTable("t1")
dataframe = spark.sqlContext.sql(\"\"\""""+query+"""\""")
    """)

import json
json_data = open('C:\Temp\MyExp.json')
data = json.load(json_data)
graph = data['Graph']

nodes = graph['ModuleNodes']
d = {i['Comment']: i for i in nodes}  
