from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    positive=[]
    negative=[]
    for count in counts:
        if len(count)!=0:
            positive.append(count[0][1])
            negative.append(count[1][1])
    plt.plot(positive,'bo-',label="Positive")
    plt.plot(negative,'go-',label="Negative")
    plt.xlabel("Time Step")
    plt.ylabel("Word count")
    plt.legend(loc="upper left")
    plt.savefig("plt.png")
    plt.show()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    content_list=[]
    with open(filename, "r") as f:
        content_list=f.read().split()
    return content_list; 



def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    def type_of_word(word):
        if word in pwords:
            return "positive"
        elif word in nwords:
            return "negative"
        
    def None_to_0(category,count):
        if count is None:
            count=0
        return sum(category,count)
        
    tweet_words= tweets.flatMap(lambda line: line.split(" "))
    mapped_words= tweet_words.map(lambda word: (type_of_word(word.lower()),1))
    total_count_step= mapped_words.reduceByKey(lambda x,y: x+y)
    total_count_step = total_count_step.filter(lambda x: (x[0]=="positive" or x[0]=="negative"))

    running_count=total_count_step.updateStateByKey(None_to_0)
    print("*************printing*******************")
    running_count.pprint()
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    total_count_step.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    #YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    
    
    


    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]

    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()

