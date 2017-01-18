CS 489 Big Data Assignment 1
============================

Question 1
----------
Pairs  
My pairs implementation runs 2 MapReduce jobs. The first counts the number of lines in the file, and how many lines each word occurs on. A word is written to an intermediate file if it occurs on a number of lines greater than the threshold. The second job counts the number of lines the pairs of words occur on. The reducer of the second job loads the intermediate file, and creates a HashMap mapping each word to the number of lines it occurs on. The reducer sums the occurence of X and Y and computes the probability of P(X, Y) by dividing by the total number of lines. P(X) is calculated by getting the count of X from the HashMap and dividing by the total number of lines. Similarly P(Y) is computed. At this point, we have all the needed information to calculate the PMI. The reducer outputs the pair of words along with their PMI and their co-occurence count.
  
Stripes
The stripes implementation also has 2 jobs, the first of which counts the number of occurences of each word as described above. 

Question 2
----------
linux.student.cs.uwaterloo.ca was used to find both running times  
Pairs Running Time: 52.368s  
Stripes Running Time: 28.399s


Question 3
----------
linux.student.cs.uwaterloo.ca was used to find both running times  
Pairs Running Time Without Combiners: 34.073s  
Stripes Running Time Without Combiners: 23.278s


Question 4
----------
38598 distinct pairs extracted


Question 5
----------
Highest PMI  
(maine, anjou)	(3.6331422, 12)  
(anjou, maine)	(3.6331422, 12)  

Lowest PMI  
(thy, you)	(-1.5303967, 11)  
(you, thy)	(-1.5303967, 11)  

Question 6
----------
Highest PMI with "tears"  
(tears, shed)	(2.1117902, 15)  
(tears, salt)	(2.0528123, 11)  
(tears, eyes)	(1.165167, 23)  

Highest PMI with "death"  
(death, father's)	(1.120252, 21)  
(death, die)	(0.75415933, 18)  
(death, life)	(0.7381346, 31)  


Question 7
----------
(hockey, defenceman)	(2.4030268, 147)  
(hockey, winger)	(2.3863757, 185)  
(hockey, goaltender)	(2.2434428, 198)  
(hockey, ice)	(2.195185, 2002)  
(hockey, nhl)	(1.9864639, 940)  


Question 8
----------
(data, storage)	(1.9796829, 100)  
(data, database)	(1.8992721, 97)  
(data, disk)	(1.7935462, 67)  
(data, stored)	(1.7868549, 65)  
(data, processing)	(1.6476576, 57)  
