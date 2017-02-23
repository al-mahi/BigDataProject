# BigDataProject
Implemntation of the article: 
Opinion Mining and Sentiment Polarity on Twitter and Correlation Between Events and Sentiment Peiman

#####Folder Structure
* BigDataProject
    * lib       --all the external file goes here
    * src       --dir of java source
        * bigdata.twitterapi        --data collection code using twitter4j
        * bigdata.mapreduce         --map reduce code for sentiment analysis
        * flume                     --flume codes and configuration if any
    * scripts                       --scripts python, shell etc
    * conf                          --hadoop configuration file

Please do no add build artifacts or IDE specific .xml files. only hadoop
configuration .xml files can be uploaded

usaege for twitter4j:
javac -c $CLASSPATH SearchQuery.java
java -c $CLASSPATH SearchQuery sec_key1 sec_key2 sec_key3 sec_key4
    


