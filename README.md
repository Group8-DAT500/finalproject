# Application to extract keyword (using Hadoop and mainly Spark)

## The raw data used

To use this program download the raw data on your machine. It can be found in [here](https://github.com/daveshap/PlainTextWikipedia)

After downloading copy the XML file on Hadoop Ditributed File System (HDFS), with this command in your terminal:

```bash
hadoop fs -put <the name of your raw data file>/*.txt  

```

## Preprocess on Hadoop

Run the following command to start the preprocessing by Hadoop :

```bash
python3 Preprocessing_on_hadoop.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop hdfs:///<the name of your raw data file>.txt --output-dir hdfs:///output --no-output

```


After the running is completed you can see the result by running this command in terminal:
```bash
hadoop fs -text /output/part* | less

```

To save your result in csv format run the command below:

```bash
hadoop fs -text /output/part*  > output.csv
```

To use this result in Spark you need to copy this csv file in HDFS.  Run this command :

```bash
hadoop fs -put output.csv /

```


## Begining with the Spark 

To start the Spark application you should save the csv file inside a delta table. to do that run the fist code block of the `Spark_code_group8.ipynb` jupyter notebook file. 

remember to replace the `/simplewiki/output_full_2g.csv` with your own path and filename:

```python 
#read in our csv file
df = spark.read.csv("/simplewiki/output_full_2g.csv", sep = ',', header = True, schema = schema)
```

later in the script the dataframe is written into an unmangaed delta table:

```python
#write the df to a unmanaged delta table. 
df.write.format("delta").mode("overwrite").save("hdfs:///table_2g")
```

After this session  of Spark is completed you have a delta table that your can run for each of the subsequent Spark optimization method.

### Keywords 

Run the code block `Keywords` to see the result on basic application. Note that to run it on your entire dataframe you shoud remove the limit(2) from the code below :

```python
#Read in data
dftest = spark.read.format("delta").load("hdfs:///table_2g").limit(2) 
```

This is an example of the output displayed in notebook:

Mean overlap: 0.5
<!-- +----------+---------------------------------------------------------------------------------------------------------------+-------+
|ID        |keywords                                                                                                       |overlap|
+----------+---------------------------------------------------------------------------------------------------------------+-------+
|1133240144|[zakraj, ek, july, ndash, september, slovene, mathematician, computer, scientist, born]                        |1      |
|1146534968|[waleswales, identified, inhabited, humans, years, evidenced, discovery, neanderthal, bontnewydd, palaeolithic]|0      |
+----------+---------------------------------------------------------------------------------------------------------------+-------+ -->


![output img](/Output.png "output image")

### Data Frame size Reduced

Run the code block `Keywords` to see the result on reduced-sized dataframe  application.

### IO- optimzation: Partitioning (36) with cacheing


Run the application on multiple partition size and see the different execution times. You can change it in this section of the script:

```python
dftest = spark.read.format("delta")\
         .option("numPartitions", 36) \  
         .option("partitionBy", "ID") \
         .load("hdfs:///table_2g")\
         .limit(2)
```

### IO-optimization: Columnar compression

Run the code block `IO-optimization: Columnar compression` to test the compression method as optimization

### Map Partition

Run the code block of MapPartition to see the result. This method has a signifcantly lower execution time.

### Using HOF

This method only runs in small datafram size. to try this method reduce the data frame size to small number of rows using the `limit()` command



### Spark UI 

Finally you can see the details of spark performance in Spark UI. For tracking the jobs connect to: http://localhost:8088/.

remember to add the port in your machine.