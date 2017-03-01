CS 489 Big Data Assignment 5
============================

When dealing with the Parquet files, I convert them into an RDD[String] after loading. This is because the text files are loaded into this format. My code is structured in such a way that RDD manipulations to execute the query are independant of the data format. There likely would have been a slight speed boost if I delt with the RDD in the format that the Parquet reader returns, but I chose to convert to RDD[String] for the sake of code clarity and scalability.
