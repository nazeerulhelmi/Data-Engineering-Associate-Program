# Twitter sentiment polarity analysis

This is actually the project given during the Data Engineering program.

The task was to create a Spark Application to perform sentiment analysis on the dataset given, and then populate the sentiment polarity.

The sentiment polarity will be classified into 3 categories; positive, neutral, and negative.

The dataset given was a csv file without header. (supposedly 8 columns)

First step in to ensure the dataset and the initial code.py were in the same folder before running Hue and Pyspark.

Using jupyter notebook, start coding from the initial code;

  - Configuring Spark Context
  - Create a function to define sentiment polarity
  - Create a pipeline using RDD transformations in the main function to create a new dataset (named: rawData), which involved;
    - Read the dataset in RDD(Resilient Distributed Dataset)
    - Identify delimiter, in this case, a comma(,)
    - Identify length of fields (no. of columns)
    - Removing empty lines (rows)
    - Rearranging fields(columns) order, if necessary
    - Result: a new dataset in an organized table (with rows and columns but without header yet)
  - To get the polarity values (continue in main function);
    - Identify the text(tweets) column
    - Map the available functions on it
    - Use TextBlob sentiment polarity
    - Categorize based on sentiment polarity
    - Result: a single column containing one of the three sentiment polarity categories per row (named: polarityData)
  -  Merge the organized dataset and the polarity column to obtain a new table of dataset with 9 columns
  -  Save it as a text file
  -  Back to jupyter notebook, create a new python file to create headers for the dataset


