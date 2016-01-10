#run like this:      sparkR --packages com.databricks:spark-csv_2.10:1.0.3 retRandomForestScript.R

library(SparkR)
sc <- sparkR.init(master = "local", sparkPackages="com.databricks:spark-csv_2.11:1.0.3")
sqlContext <- sparkRSQL.init(sc)

#read training data CSV from Spark sqlContext into SparkR RDD
sparkDataFrame <- read.df(sqlContext, "trainingdata.csv", "com.databricks.spark.csv", header="true")

#Need to convert to local R dataFrame, therefore not taking advantage of Spark distribution! 
fullDataFrame <- collect(sparkDataFrame)

#making sure we just have columns we need
selectData <- fullDataFrame[,c("INCREMENT","A","B","C","D","E","F")]


# function from Doctor Google to convert dataframe. Need to convert dataframe from Spark in characters to numeric type.
# SparkR dataframes make everything characters by default, and it does not seem to support casting at read time yet
convert.dataframecolumns <- function(obj, type){
  FUN1 <- switch(type,
                 character = as.character,
                 numeric = as.numeric,
                 factor = as.factor)
  out <- lapply(obj, FUN1)
  as.data.frame(out)
}
selectData <- convert.dataframecolumns(selectData, "numeric")

# train model based on training data
library(caret)
model_rf_ret <- train(INCREMENT ~ .,  method="rf", data=selectData) 


# now we want to read input values CSV from Spark sqlContext into SparkR RDD
inputDataFrame <- read.df(sqlContext, "clientFile.csv", "com.databricks.spark.csv", header="true")
#Need to convert to local R dataFrame, therefore not taking advantage of Spark distribution! 
inputLocalRDataFrame <- collect(inputDataFrame)
#Need to convert KPI columns to numeric
inputLocalRDataFrame[, c(3:8)] <- sapply(inputLocalRDataFrame[, c(3:8)], as.numeric)


predicted_output<- predict(model_rf_ret, newdata = inputLocalRDataFrame)


# add the Predicted values as extra column named PREDICTED (last column) to original input cell data dataframe
inputLocalRDataFrame[,"PREDICTED"] <- predicted_output

# create SparkR RDD dataframe from local R dataframe
outputSparkRDataframe<- createDataFrame(sqlContext, inputLocalRDataFrame)
#  we could choose to save to parquet file like :     write.df(outputSparkRDataframe, path="people.parquet", source="parquet", mode="overwrite")

head(inputLocalRDataFrame)

sparkR.stop()