# Capstone

I took the first approach in building the Data Lake on the data on immigration to the United States provided by Udacity.

A business consulting firm specialized in data warehouse services through assisting the enterprises with navigating their data needs and creating strategic operational solutions that deliver tangible business results is contacted by U.S Customs & Border Protection Department. Specifically, they want help with the modernization of department's data warehousing infrastructure by improving performance and ease of use for end users, enhancing functionality, decreasing total cost of ownership while making it possible for real-time decision making. In total, the department is asking for a full suite of services includes helping department with data profiling, data standardization, data acquisition, data transformation and integration.

The U.S. Customs and Border Protection needs help to see what is hidden behind the data flood. The consulting firm aim to model and create a brand new analytics solution on top of the state-of-the-art technolgies available to enable department to unleash insights from data then making better decisions on immigration policies for those who came and will be coming in near future to the US


The solution implemented is based on cloud AWS. In which all of the datasets were preprocessed using apache spark and using AWS S3 staging area to store those preprocessed datasets. Then, it is loaded into a Amazon Redshift cluster using an Apache Airflow pipeline that transfers and checks the quality of the data to finally provide the department a Data Lake for their convenient analysis. 
