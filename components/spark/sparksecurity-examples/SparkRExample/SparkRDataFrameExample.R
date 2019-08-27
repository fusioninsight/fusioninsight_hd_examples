
# To run this example use
# spark-submit --master yarn-client --files ./user.keytab,./jaas-zk.conf,/opt/client/Spark/spark/conf/kdc.conf SparkRDataFrameExample.R /tmp/people2.json
# Load SparkR library into your R session  
library(SparkR)

args <- commandArgs(trailing = TRUE)

if (length(args) != 1) {
    print("Usage: SparkRDataFrameExample.R <path-of-jsonFile.json>")
    q("no")
}

## Initialize SparkContext 
sc <- sparkR.init(appName = "SparkR-data-example")

## Initialize SQLContext
sqlContext <- sparkRSQL.init(sc)

jsonFilePath <- args[[1]]


#=====Create SparkR DataFrame from Data Source =====
peopleDF <- read.df(sqlContext, jsonFilePath, source = "json", header = "true")

# Print the schema of this Spark DataFrame 
printSchema(peopleDF)


#=====register Table, and table select=====

# Using SQL to select columns of data
# First, register the flights DataFrame as a table
registerTempTable(peopleDF, "peopleTable")
destDF <- sql(sqlContext, "select * from (select name,sum(time) as totalStayTime from peopleTable where gender = 'female' group by name ) test where totalStayTime >120")

# Use collect to create a local R data frame
local_df <- collect(destDF)

# Print the newly created local data frame
print(local_df)

# Stop the SparkContext now
sparkR.stop()
