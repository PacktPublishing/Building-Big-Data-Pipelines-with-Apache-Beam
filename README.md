# Building Big Data Pipelines with Apache Beam

<a href="https://www.packtpub.com/product/building-big-data-pipelines-with-apache-beam/9781800564930?utm_source=github&utm_medium=repository&utm_campaign=9781800564930"><img src="https://static.packt-cdn.com/products/9781800564930/cover/smaller" alt="Building Big Data Pipelines with Apache Beam" height="256px" align="right"></a>

This is the code repository for [Building Big Data Pipelines with Apache Beam](https://www.packtpub.com/product/building-big-data-pipelines-with-apache-beam/9781800564930?utm_source=github&utm_medium=repository&utm_campaign=9781800564930), published by Packt.

**Use a single programming model for both batch and stream data processing**

## What is this book about?
This book describes both batch processing and real-time processing pipelines. You’ll learn how to implement basic and advanced big data use cases with ease and develop a deep understanding of the Apache Beam model. In addition to this, you’ll discover how the portability layer works and the building blocks of an Apache Beam runner.

This book covers the following exciting features: 
* Understand the core concepts and architecture of Apache Beam
* Implement stateless and stateful data processing pipelines
* Use state and timers for processing real-time event processing
* Structure your code for reusability
* Use streaming SQL to process real-time data for increasing productivity and data accessibility
* Run a pipeline using a portable runner and implement data processing using the Apache Beam Python SDK
* Implement Apache Beam I/O connectors using the Splittable DoFn API

If you feel this book is for you, get your [copy](https://www.amazon.com/dp/1801077053) today!

<a href="https://www.packtpub.com/?utm_source=github&utm_medium=banner&utm_campaign=GitHubBanner"><img src="https://raw.githubusercontent.com/PacktPublishing/GitHub/master/GitHub.png" 
alt="https://www.packtpub.com/" border="5" /></a>


## Instructions and Navigations
All of the code is organized into folders.

The code will look like the following:
```
ClassLoader loader = FirstPipeline.class. 
getClassLoader(); 
String file = loader.getResource("lorem.txt").getFile();
List<String> lines = Files.readAllLines( Paths.get(file), StandardCharsets.UTF_8);
```

**Following is what you need for this book:**
This book is for data engineers, data scientists, and data analysts who want to learn how Apache Beam works. Intermediate-level knowledge of the Java programming language is assumed.	

With the following software and hardware list you can run all code files present in the book (Chapter 1-7).

### Software and Hardware List

| Chapter  | Software required                   | OS required                        |
| -------- | ------------------------------------| -----------------------------------|
| 1-7	     | Java 11, Python 3                   | Windows, Mac OS X, and Linux (Any) |
| 1-7      | Bash                                | Windows, Mac OS X, and Linux (Any) |
| 1-7      | Docker                              | Windows, Mac OS X, and Linux (Any) |


We also provide a PDF file that has color images of the screenshots/diagrams used in this book. [Click here to download it](https://static.packt-cdn.com/downloads/9781800564930_ColorImages.pdf).


### Related products <Other books you may enjoy>
* Data Engineering with Apache Spark, Delta Lake, and Lakehouse [[Packt]](https://www.packtpub.com/product/data-engineering-with-apache-spark-delta-lake-and-lakehouse/9781801077743?utm_source=github&utm_medium=repository&utm_campaign=9781801077743) [[Amazon]](https://www.amazon.com/dp/1801077746)

* Data Engineering with Python [[Packt]](https://www.packtpub.com/product/data-engineering-with-python/9781839214189?utm_source=github&utm_medium=repository&utm_campaign=9781839214189) [[Amazon]](https://www.amazon.com/dp/183921418X)

## Get to Know the Author
**Jan Lukavský**
is a freelance big data architect and engineer who is also a committer of Apache Beam. He is a certified Apache Hadoop professional.
He is working on open source big data systems combining batch and streaming data pipelines in a unified model, enabling the rise of real-time, data-driven applications.
