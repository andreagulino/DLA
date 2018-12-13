# DLA
DLA Sequence Pattern Mining

Reference paper

Usage
```
java -jar dla.jar [OPTIONS]
```

The following options are available: 


| Option  | Value |
| ------------- | ------------- |
| -minLen  | integer representing the minimum length of patterns that should be found in the sequence database |
| -maxLen   | integer representing the maximum length of patterns that should be found in the sequence database  |
| -input   | path tho the input sequence database (text file having as first column the sequence-id, second column the sequence string). HDFS paths are supported.  |
| -output   | path to the output folder  |
| -sparkMaster   | address of the spark master (default: local)  |

