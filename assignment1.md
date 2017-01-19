CS 489 Big Data Assignment 1
============================

Question 1
----------

Briefly describe in prose your solution, both the pairs and stripes implementation. For example: how many MapReduce jobs? What are the input records? What are the intermediate key-value pairs? What are the final output records? A paragraph for each implementation is about the expected length.

**Pairs**  
My pairs implementation runs 2 MapReduce jobs. The first counts the number of lines in the file, and how many lines each word occurs on. A word is written to an intermediate file if it occurs on a number of lines greater than the threshold. The format of the intermediate file is as follows.  
word count  
The second job counts the number of lines the pairs of words occur on. The reducer of the second job loads the intermediate file, and creates a HashMap mapping each word to the count. The reducer sums the occurence of X and Y and computes the probability of P(X, Y) by dividing by the total number of lines. P(X) is calculated by getting the count of X from the HashMap and dividing by the total number of lines. Similarly P(Y) is computed. At this point, we have all the needed information to calculate the PMI. The reducer outputs the pair of words along with their PMI and their co-occurence count. The final output file is formatted as follows.  
(word1, word2) (PMI, count)  
  
**Stripes**  
The stripes implementation also has 2 jobs, the first of which counts the number of occurences of each word and writes to an intermediate file as described above. The second mapper takes in a line and outputs each distinct word alongwith a map containing all other distinct words as the key and 1 as the value. For example.  
word1 {word2=1, word3=1}
The combiner does an element wise sum of the map. The reducer loads the word counts from the first job into a HashMap to be used to calculate the PMI. It does an element wise sum like the combiner, and then loops over the elements in the map. If the element's count exceeds the threshold, then the PMI is calculated. The output is a word with a map of co-occuring words, each of which mapping to a PMI, co-occurence count pair. The final output file is formatted as follows.  
word1 {word2=(PMI, count), word3=(PMI, count)}  


Question 2
----------
linux.student.cs.uwaterloo.ca was used to find both running times  
Pairs Running Time: 49.499s  
Stripes Running Time: 24.201s  


Question 3
----------
linux.student.cs.uwaterloo.ca was used to find both running times  
Pairs Running Time Without Combiners: 59.023s  
Stripes Running Time Without Combiners: 28.506s  


Question 4
----------
38599 distinct pairs extracted


Question 5
----------
**Highest PMI**  
(maine, anjou)	(3.6331422, 12)  
(anjou, maine)	(3.6331422, 12)  

**Lowest PMI**  
(thy, you)	(-1.5303967, 11)  
(you, thy)	(-1.5303967, 11)  

Question 6
----------
**Highest PMI with "tears"**  
(tears, shed)	(2.1117902, 15)  
(tears, salt)	(2.0528123, 11)  
(tears, eyes)	(1.165167, 23)  

**Highest PMI with "death"**  
(death, father's)	(1.120252, 21)  
(death, die)	(0.75415933, 18)  
(death, life)	(0.7381346, 31)  


Question 7
----------
**Highest PMI with "hockey"**
(hockey, defenceman)	(2.4030268, 147)  
(hockey, winger)	(2.3863757, 185)  
(hockey, goaltender)	(2.2434428, 198)  
(hockey, ice)	(2.195185, 2002)  
(hockey, nhl)	(1.9864639, 940)  


Question 8
----------
**Highest PMI with "data"**
(data, storage)	(1.9796829, 100)  
(data, database)	(1.8992721, 97)  
(data, disk)	(1.7935462, 67)  
(data, stored)	(1.7868549, 65)  
(data, processing)	(1.6476576, 57)  
