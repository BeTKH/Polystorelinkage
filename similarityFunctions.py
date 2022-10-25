import numpy as np
import nltk
from myAttribute import myAttribute
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
import re
import config

def get_wordnet_pos(word): #https://www.machinelearningplus.com/nlp/lemmatization-examples-python/
    """Map POS tag to first character lemmatize() accepts"""
    tag = nltk.pos_tag([word])[0][1][0].upper()
    tag_dict = {"J": wordnet.ADJ,
                "N": wordnet.NOUN,
                "V": wordnet.VERB,
                "R": wordnet.ADV}

    return tag_dict.get(tag, wordnet.NOUN)

#gibt wahr zurück, wenn Typ 1 und Typ 2 äquivalent sind
#returns true if type 1 and type 2 are equivalent
def compareTypes(type1, type2):
    typeDict = config.typeDict
    typeLists = config.typeLists


    if type1 == type2:
        return True
    index = -1
    for key in typeDict:
        if re.search(key, type1):
            index = typeDict[key]
            break
    if index < 0:
        return False

    for entry in typeLists[index]:
        if re.search(entry, type2):
            return True

    return False

def compareValuesStrict(value1, value2):
    if type(value1) != type(value2):
        return -2;
    elif isTypeOdered(type(value1)):
        if value1 < value2:
            return -1
        elif value1 == value2:
            return 0
        else:
            return 1
    else:
        return -3

def isTypeOdered(Type):
    orderedTypes = [str, int, float, complex]
    return Type in orderedTypes

#returns similarity of 2 values
#Vergleicht 2 Werte, Ergebnis im Bereich [0,1]
#Compares 2 values, result in range [0,1]
def compareValues(value1, value2):
    if (type(value1) == list or type(value1) == tuple) and (type(value2) == list or type(value2) == tuple):
        return getIterableSimilarities(value1, value2)

    if type(value1) != type(value2):
        return 0;
    if type(value1) == str:
        return getWordSimilarity(value1, value2)
    else:
        return getDefaultSimilarity(value1, value2)

#returns Matrix with similarityrates between 2 lists
def compareLists(list1, list2):
    #config.init(configDic)
    #if not compareTypes(Attribute1.dataType, Attribute2.dataType):
    #    return np.array([[0]*len(list2)]*len(list1))

    i = 0
    j = 0
    matches = 0
    ordered = True
    while i < len(list1) and j < len(list2):
        similarity = compareValuesStrict(list1[i],list2[j])
        if similarity == 0:
            i = i+1
            j = j+1
            matches = matches + 1
        elif similarity == -1:
            i = i+1
        elif similarity == 1:
            j = j+1
        elif similarity == -2:
            return 0
        else:
            ordered = False
            break
    
    if not ordered:
        matches = 0
        for element in list1:
            if element in list2:
                matches = matches+1

    if len(list1)<=0 or len(list2) <= 0:
        return 0
    else:
        return matches/min(len(list1), len(list2))

#Vergleich von 2 Strings durch Hamming und Leventheindistance
#Comparison of 2 strings by Hamming and Leventheindistance
def getWordSimilarity(word1, word2):
    result = 0;
    if len(word1) == len(word2):
          result = 1-hamming(word1, word2)/len(word1)
    else:
        if len(word1) <= config.levMaxLen and len(word2) <= config.levMaxLen:
            result = 1-lev(word1,word2)/max(len(word1),len(word2))
        else: 
            result = 0;
    return result

# Erweiterung durch lemmartization
# Extension through lemmartization
def getExtendedWordSimilarity(word1, word2):
    config.init()
    lemmatizer = WordNetLemmatizer()
    word1_lemma = lemmatizer.lemmatize(word1, get_wordnet_pos(word1))
    word2_lemma = lemmatizer.lemmatize(word2, get_wordnet_pos(word2))
    for synset in wordnet.synsets(word1):
        for lemma in synset.lemmas():
            if word2_lemma == lemma.name():
                return 1
    
    if word2 in getSynonyms(word1):
        return 1;

    return getWordSimilarity(word1_lemma, word2_lemma)

# Vergleich von sonstigen Werten
# Comparison of other values
def getDefaultSimilarity(value1, value2):
    if value1 == value2:
        return 1
    else:
        return 0


def getIterableSimilarities(list1, list2):
    matches1 = 0
    for element1 in list1:
        for element2 in list2:
            if compareValues(element1, element2) >= 1:
                matches1 = matches1 + 1
                break;

    matches2 = 0
    for element2 in list2:
        for element1 in list1:
            if compareValues(element2, element1) >= 1:
                matches2 = matches2 + 1
                break;
    return max(matches1/len(list1), matches2/len(list2))

#Levensteindistance of two words
#Levenstheindistanz zweier strings
#Levenstheindistance of two strings
def lev(word1, word2):
    if len(word1) == 0:
        return len(word2);
    if len(word2) == 0:
        return len(word1);
    if word1[0]==word2[0]:
        return lev(word1[1:], word2[1:])
    return 1 + min(lev(word1,word2[1:]),lev(word1[1:],word2),lev(word1[1:], word2[1:]))

#Hammingdistance of two words
#Hammingdistanz zweier strings
def hamming(word1, word2):
    distance = 0
    for i in range(len(word1)):
        if word1[i]!= word2[i]:
            distance+=1

    return distance

def getSynonyms(word):
    synonyms = config.synonyms
    for Class in synonyms:
        if word in Class:
            return Class
    return []