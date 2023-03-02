from textblob import TextBlob
from pyspark import SparkConf, SparkContext
import re



def abb_en(line):
   abbreviation_en = {
    'u': 'you',
    'thr': 'there',
    'asap': 'as soon as possible',
    'lv' : 'love',    
    'c' : 'see'
   }
   
   abbrev = ' '.join (abbreviation_en.get(word, word) for word in line.split())
   return (abbrev)

def remove_features(data_str):
   
    url_re = re.compile(r'https?://(www.)?\w+\.\w+(/\w+)*/?')    
    mention_re = re.compile(r'@|#(\w+)')  
    RT_re = re.compile(r'RT(\s+)')
    num_re = re.compile(r'(\d+)')
    
    data_str = str(data_str)
    data_str = RT_re.sub(' ', data_str)  
    data_str = data_str.lower()  
    data_str = url_re.sub(' ', data_str)   
    data_str = mention_re.sub(' ', data_str)  
    data_str = num_re.sub(' ', data_str)
    return data_str

#Function for sentiment polarity classification
def polarity(value):
    if value > 0:
        return "+ve"
    elif value == 0:
        return "neu"
    else:
        return "-ve"
  
   
#Write your main function here
def main(sc, file):
    #read > delimiter > length columns > empty lines > rearrange > verify
    rawData = sc.textFile(file)\
    .map( lambda x: x.split(",") )\
    .filter( lambda x: len(x) == 8 )\
    .filter( lambda x: len(x[0]) > 1 )\
    .map( lambda x: (x[4], x[0], x[2], x[1], x[3], x[5], x[6], x[7]) )
    #print(rawData.take(1))
    
    #polarity > from rearranged columns > get text column > remove feature, abbreviation > do textblob
    polarityData = rawData.map( lambda x: x[0] )\
    .map( lambda x: remove_features(x) )\
    .map( lambda x: abb_en(x) )\
    .map( lambda x: TextBlob(x).sentiment.polarity )\
    .map( lambda x: polarity(x) )
    #print(polarityData.take(2))
    
    #Merge polarity(at start, x[0]) and raw > remove single and double quotes
    mergeData = polarityData.zip(rawData)\
    .map( lambda x: str(x).replace("'","").replace('"','') )
    #print(mergeData.take(2))
    
    #save textfile
    mergeData.saveAsTextFile("De22c4exam")
    
if __name__ == "__main__":
    conf = SparkConf().setMaster("local[1]").setAppName("examC4")
    sc = SparkContext(conf = conf)
    file = "starbuck_v1.csv"
    main(sc, file)
    sc.stop()
