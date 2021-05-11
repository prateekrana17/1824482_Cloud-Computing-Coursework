#AWS Elastic Map Reduce: Word Count Application
#SID: 1824482

import os
import math
import sys
import re
import argparse
import findspark
findspark.init()
from pyspark.sql.functions import col
from pyspark.sql import SparkSession


#This function removes tokenizers and punctuations from the text and replaces them with space
def removeCharacters(lines):

    # if lines is not empty
    if lines != "":

        # convert to lower case
        lines = lines.lower()
        #replace non-alphanumeric characters with space

        lines = re.sub("[^a-zA-Z]"," ",lines)
        
    
        # join multiple spaces to one space
        lines = " ".join(lines.split())

    # Return lines without tokenizers
    return lines


#This functions splits lines to words and adds them to array
def getWords(lines):

    array_of_words = []

    # iterating through the words
    for word in lines.split():

        # To skip individual characters
        if(len(word) > 1):
            array_of_words.append(word)

    # Return the array of words
    return array_of_words


#This function returns an array if letters by splitting words
def getLetters(word):

    array_of_letters = []
    s_characters=['-','!','?',':',';','|','(',')','{','}','#','%','[',']','_','"','@']
    # Iterate through the letters
    for letter in word.split():
 
        # Check if the letter is not a tokenizer
        if(letter not in s_characters):
            array_of_letters.extend(letter)

    # Return the array of letters
    return array_of_letters


#This function returns the thresholds for splitting the array of words/letters
def getThreshold(distinct_Items):

    # percentage for calculating thresholds
    upper_Percentage = 0.05
    middle_Percentage = 0.05
    low_Percentage = 0.05
    max_Percentage  = 1.0
    offset_Percentage = max_Percentage - (upper_Percentage + middle_Percentage + low_Percentage)

    # threshold calculation
    popular_Threshold        = int(math.ceil(distinct_Items * upper_Percentage))
    common_lower_Threshold    = int(math.floor(distinct_Items * (upper_Percentage + offset_Percentage / 2) ))
    common_upper_Threshold    = int(math.ceil(distinct_Items * (max_Percentage - low_Percentage - offset_Percentage / 2) ))
    rare_Threshold           = int(math.floor(distinct_Items * (max_Percentage - low_Percentage) ))

    # Returning the thresholds
    return (popular_Threshold, common_lower_Threshold, common_upper_Threshold, rare_Threshold)


# Main function

def Main(bucketName, inputFolder, outputFolder, sampleName):

    # location of input sample files
    sampleInputPath = os.path.join(bucketName, inputFolder, outFolder, sampleName)

    # naming the output files
    outputFileName = "output-{0}".format(sampleName)
    outputPath = os.path.join(bucketName, outputFolder, outputFileName)
    
    
    print("The output will be saved in the file: {0}".format(outputFileName))
    f = open(outputFileName,'w')
    sys.stdout = f
    s_characters=['-','!','?',':',';','|','(',')','{','}','#','%','[',']','_','"','@']
 

    # Creating a spark session
    with SparkSession.builder.getOrCreate() as spark:
        
        # Reading sample file into a RDD 
        linesRDD = spark.sparkContext.textFile(sampleInputPath)

        # Removing tokenizers
        linesRDD_cleaned = linesRDD.map(lambda lines: removeCharacters(lines))

        # getting words from each line
        wordsRDD = linesRDD_cleaned.flatMap(lambda lines: getWords(lines))

        # Getting letters from each line and removing spaces and tokenizers 
        lettersRDD = linesRDD_cleaned.flatMap(lambda lines: [char for char in lines])
        #lettersRDD = lettersRDD.filter(lambda char: char!=' ' and char!='-')
        lettersRDD = lettersRDD.filter(lambda char: char!=' ' and char not in s_characters)

        # Obtaining word frequencies, re-arranging and sorting
        word_frequencyRDD = wordsRDD.map(lambda word: (word,1)).reduceByKey(lambda word, freq: word + freq)
        word_frequencyRDD = word_frequencyRDD.map(lambda x:(x[1], x[0]))
        word_frequencyRDD = word_frequencyRDD.sortByKey(False)
        word_frequencyRDD = word_frequencyRDD.zipWithIndex()
        word_frequencyRDD = word_frequencyRDD.map(lambda x: (x[1]+1, x[0][1], x[0][0]))

        # Obtaining letter frequencies, re-arranging and sorting
        letter_frequencyRDD = lettersRDD.map(lambda letter: (letter,1)).reduceByKey(lambda letter, freq: letter + freq)
        letter_frequencyRDD = letter_frequencyRDD.map(lambda x:(x[1], x[0]))
        letter_frequencyRDD = letter_frequencyRDD.sortByKey(False)
        letter_frequencyRDD = letter_frequencyRDD.zipWithIndex()
        letter_frequencyRDD = letter_frequencyRDD.map(lambda x: (x[1]+1, x[0][1], x[0][0]))
        
        # calculating the total number of distinct words and letters
        total_number_of_words = len(wordsRDD.collect())
        total_distinct_words = len(word_frequencyRDD.collect())
        total_distinct_letters = len(letter_frequencyRDD.collect())

        # Getting the words threshold values
        (popular_word_Threshold,
            common_word_lower_Threshold, 
            common_word_upper_Threshold, 
            rare_word_Threshold) = getThreshold(total_distinct_words)

        # Getting the letters threshold values
        (popular_letter_Threshold,
            common_letter_lower_Threshold, 
            common_letter_upper_Threshold, 
            rare_letter_Threshold) = getThreshold(total_distinct_letters)

        print("---------------------------------------------------------------------------------------------")
        print("SID: 1824482")
        print("Output for file: {0}".format(sampleName))
        print("---------------------------------------------------------------------------------------------")
        print("'total number of words'= {0}".format(total_number_of_words))
        print("'total number of distinct words'= {0}".format(total_distinct_words))
        print("'popular_threshold_word': {0}".format(popular_word_Threshold))
        print("'common_threshold_l_word': {0}".format(common_word_lower_Threshold))
        print("'common_threshold_u_word': {0}".format(common_word_upper_Threshold))
        print("'rare_threshold_word': {0}".format(rare_word_Threshold))
        print("---------------------------------------------------------------------------------------------")

        # Data frame for all the words
        df_Words = spark.createDataFrame(word_frequencyRDD,schema=["Rank", "Word", "Frequency"])

        # Data frame for popular words
        df_popular_Words = df_Words.where(col("Rank").between(1, popular_word_Threshold))
        # Data frame for common words
        df_common_Words = df_Words.where(col("Rank").between(common_word_lower_Threshold, common_word_upper_Threshold))
        # Data frame for rare words
        df_rare_Words = df_Words.where(col("Rank").between(rare_word_Threshold, total_distinct_words))

        print("")
        print("Popular Words")
        df_popular_Words.show(df_popular_Words.count())
        print("Common Words")
        df_common_Words.show(df_common_Words.count())
        print("Rare Words")
        df_rare_Words.show(df_rare_Words.count())

        print("---------------------------------------------------------------------------------------------")
        print("'total number of distinct letters': {0}".format(total_distinct_letters))
        print("'popular_threshold_letter': {0}".format(popular_letter_Threshold))
        print("'common_threshold_l_letter': {0}".format(common_letter_lower_Threshold))
        print("'common_threshold_u_letter': {0}".format(common_letter_upper_Threshold))
        print("'rare_threshold_letter: {0}'".format(rare_letter_Threshold))
        print("---------------------------------------------------------------------------------------------")

        # Data frame for all the letters
        df_Letters = spark.createDataFrame(letter_frequencyRDD,schema=["Rank","Letter","Frequency"])

        # Data frame for popular letters
        df_popular_Letters = df_Letters.where(col("Rank").between(1, popular_letter_Threshold))
        # Data frame for common letters
        df_common_Letters = df_Letters.where(col("Rank").between(common_letter_lower_Threshold, common_letter_upper_Threshold))
        # Data frame for rare letters
        df_rare_Letters = df_Letters.where(col("Rank").between(rare_letter_Threshold, total_distinct_letters))

        print("")
        print("Popular Letters")
        df_popular_Letters.show(df_popular_Letters.count())
        print("Common Letters")
        df_common_Letters.show(df_common_Letters.count())
        print("Rare Letters")
        df_rare_Letters.show(df_rare_Letters.count())
        print("---------------------------------------------------------------------------------------------")

        f.close()
        sys.stdout = sys.__stdout__

        print("Success!, results stored in: {0}".format(outputFileName))

        # Send the output file to s3 bucket
    os.system("aws s3 mv {0} {1}".format(outputSampleFileName, outputPath))

    

# input arguments
# starting point
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('bucketName')
    parser.add_argument('inputFolder')
    parser.add_argument('outputFolder')
    parser.add_argument('sampleName')
    args = parser.parse_args()

    Main(args.bucketName, args.inputFolder, args.outputFolder, args.sampleName)
