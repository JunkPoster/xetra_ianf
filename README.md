# Udemy Project
### "Writing Production-ready ETL Pipelines in Python/Pandas"
<b> Course by Jan Schwarzlose</b>, project by Ian Featherston

This repository is the product of what was worked on in the course. My work is a slight deviation from the course, as I tried to document more thoroughly to help myself learn some of the new concepts.

If I were to refactor this code, I would consider parameterizing the various  status/error messages for more flexibility and easier maintenance. It would  also help with consistency between the unit tests and the actual code. 

I would also consider adding more error handling and logging to the code, and  likely a more detailed profiler to monitor performance.

I think there's a lot that could be modularized as well, such as the S3BucketConnector class, which could be made more generic and used for other projects. I would also add more unit tests.