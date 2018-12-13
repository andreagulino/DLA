# DLA
DLA Sequence Pattern Mining

Download the reference paper: https://github.com/andreagulino/DLA/raw/master/DLA.pdf

Usage
```
java -jar dla.jar [OPTIONS]
```

The following options are available: 


| Option  | Value |
| -------------------- |------|
| -minLen  | integer representing the minimum length of patterns that should be found <br>in the sequence database |
| -maxLen   | integer representing the maximum length of patterns that should be found in <br> the sequence database  |
| -input   | path tho the input sequence database (text file having as first column<br> the sequence-id, second column the sequence string). HDFS paths are supported.  |
| -output   | path to the output folder  |
| -minPartitions   | number of partitions to split the input file |
| -sparkMaster   | address of the spark master (default: local)  |

