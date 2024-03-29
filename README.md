What is a data pipeline?
A data pipeline is a series of tasks, such as transformations, filters, aggregations, and merging multiple sources,
before outputting the processed data into some target. 

# How do we create a robust pipeline?
1: Clearly defined expectations
    a): The data it is processing and the results it is expected to produce.
    b): this includes specifying types and sources of data, as well as the desired output format and any required transformations or aggregations.
2: Scalable architecture
    a): Can handle increasing volumes of data without degradation in performance. 
    b): using distributed systems or implementing efficient algorithms to process the data promptly
3: Reproducible and Clear
    a): produce the same results each time it is run, and the steps involved in processing the data should be 
        documented and easy to understand.

# Pre-work - understanding your data
1: input: 
    a): clear definitions of the input data, data structures, the prevalence of corrupted data, frequency of new data creation
    b): likelihood of data corruption, how can it be accounted for within the pipline
2: output:
    a): structural requirements of the output

## What is an ETL data pipeline?
ETL stands for Extract, Transform, and Load. In an ELT process, data is first extracted from a source,
then transformed and formatted in a specific way, and finally loaded into a final storage location. 

# How to choose between ETL and ELT:
    a): Data volume: If the volume of data is very large, ELT might be more efficient because the transformation step can be done in parallel within the target system
    b): Data transformation requirements: if the data needs to undergo complex transformations it might be easier to perform the transformation int the target system
    c): Source system capabilities: if the source system is not able to perform the necessary transformations
    d): Target system capabilities: if the target system is not able to efficiently handle the load phase the ETL, ELT might be a better option
    e): Data latency: if a real-time data movement is required, ELT might be a better choice.

# Types of ETL pipelines:
    a): Batch processing: 
        Batch processing is a method of data proccessing that involves dividing a large volume of data into smaller pieces, or batches, and processing each batch separately. 
    b): Streaming:
        Real-time data solutions are necessary when a project needs to immediately process fresh data.
        Streaming methods are often used in these situations are they allow data to flow continuously, which may be vairable and subject to sudden changes in structure. 
    c): Cloud-native
        AWS, google cloud platform, Microsoft



