# DWH Coding Challenge results

Each history table and joined table printed out to the stdout.


## 1. History tables

I've printed out history tables for each of the event tables.

![img_4.png](img_4.png)
![img_1.png](img_1.png)
![img_2.png](img_2.png)

## 2. Joined table
2 screenshots provided here of printed out tables

![img.png](img.png)
![img_5.png](img_5.png)

## 3. Number of transactions

Total of 8 transactions. 
6 transactions for the first card c1, 2 transactions after tying up second card.

examples of transactions with credit_used:
1. 0 -> 12000
2. 12000 -> 19000
3. 19000 -> 0
etc. 

![img_6.png](img_6.png)

## 4. Thoughts on the process, future work

I would do an interface implementation of access to the data - to transfer then to nosql (i.g. hbase) or hdfs. And implement this as a microservice.
Also introduce it to the k8s or cloud orcestration service.