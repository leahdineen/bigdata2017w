CS 489 Big Data Assignment 4
============================

I completed the multi-source personalized pagerank on both linux.student and Altiscale. The pagerank uses regular combiners and hash partitioning.
Please note that the range, useCombiner, useInmapCombiner command line options were removed to simplify the code.


Output from running on Altiscale cluster
```
Source: 73273  
0.15509 73273  
0.00437 5042916  
0.00332 22218  
0.00289 259124  
0.00225 89585  
0.00178 5752184  
0.00176 498093  
0.00170 20798  
0.00148 3434750  
0.00139 178880  


Source: 73276  
0.15575 73276  
0.00501 5042916  
0.00364 22218  
0.00297 73273  
0.00199 20150591  
0.00169 169136  
0.00147 64646  
0.00146 3434750  
0.00137 216932  
0.00131 6897402  
```
