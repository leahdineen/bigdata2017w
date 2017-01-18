CS 489 Big Data Assignment 1
============================

Question 1
----------
Pairs  
My pairs implementation runs 2 MapReduce jobs. The first counts the number of lines in the file, and how many lines each word occurs on. A word is written to an intermediate file if it occurs on a number of lines greater than the threshold. The second job counts the number of lines the pairs of words occur on. The reducer of the second job loads the intermediate file, and creates a HashMap mapping each word to the number of lines it occurs on. The reducer sums the occurence of X and Y and computes the probability of P(X, Y) by dividing by the total number of lines. P(X) is calculated by getting the count of X from the HashMap and dividing by the total number of lines. Similarly P(Y) is computed. At this point, we have all the needed information to calculate the PMI. The reducer outputs the pair of words along with their PMI and their co-occurence count.
  
Stripes

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

Lowest PMI  


Question 6
----------
Highest PMI with "tears"  


Highest PMI with "death"  



Question 7
----------


Question 8
----------

