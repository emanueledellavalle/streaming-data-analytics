# Streaming Data Analytics

This is the git repository of the "Streaming Data Engineering" course of Politecnico di Milano. It consists in two parts. 
The first is about Streaming Data Engineering while the second is about Streaming Data Science.

## Streaming Data Engineering

This Streaming Data Engineering part of the course covers theoretical and practical aspects of Event-Based Systems (EBS), Data Stream Management Systems (DSMS), and Complex Event Processing (CEP). It uses the Event Processing Language (EPL) to illustrate typical DSMS and CEP operations. This segment equips students with essential skills for real-time data processing and analysis.

**Event-Based Systems** emphasizes efficient data collection and integration from various sources and redistribution of events to sinks, ensuring high throughput and fault tolerance. In particular, it presents Apache Kafka because it enables handling large-scale data streams, making it essential for robust data pipeline architectures. 

* Stream ingestion and Event-based Architectures
* Apache Kafka as an essential building block of streaming data pipelines
* hands-on producer-consumer APIs
* Case study: ingesting data for a global retail

The **Event Processing Language** is a rich language that allows users to specify typical operations present in DSMS and CEP easily. It provides the ability to filter, aggregate, and transform data as it arrives. Moreover it allows identifying meaningful patterns and relationships in real-time data streams, enabling the detection of complex events from simple events.

* Filtering, aggregating, and windowing data streams
* Joining Data streams
* Using the "every" clause and pattern guards to tame the *torrent effect**
* Hands-on with EPL on case studies in Retail, IoT, and Industry 4.0

Scaling stream processing with **Apache Spark Structured Streaming** focuses on the real-time processing and analysis of large datasets. Apache Spark's structured streaming capabilities provide scalability, fault tolerance, and high performance, enabling advanced analytics on streaming data.

* Efficient DSMS operations 
* Hard to implement CEP operations
* Hands-on practice with Spark Structured Streaming

## Streaming Data Science

The Streaming Data Science part of the course covers the theoretical and practical aspects of time Series Analytics, Streaming Machine Learning, and Continuous Learning, equipping students with essential skills for real-time data analysis.

**Time Series Analysis** (TSA) focuses on developing models to analyze and forecast data points in chronological order. TSA approaches capture temporal dependencies and patterns, such as trends and seasonality, enabling accurate predictions and insights. TSA methods are also adept at handling non-stationarity in data.

* ⁠Stationarity and ⁠Decomposition
* Time Series Forecasting baselines
* ⁠Temporal Dependence and SARIMAX forecasting models
* ⁠Advanced approaches to Time Series Forecasting
* ⁠Hands-on with the Statsmodels e Darts Python libraries

**Streaming Machine Learning** (SML) proposes models able to learn from data streams over-time. These models are incrementally updated as soon new data becomes available, avoiding retraining them from scratch, and adapting to many forms of non-stationarity (a.k.a. Concept Drift).

* ⁠Learning one sample at a time
* ⁠⁠Learning in the presence of Concept Drifts
* ⁠Streaming algorithms for classification and regression
* ⁠Hands-on with the River Python library

**Continual Learning** (CL) proposes strategies to address the problem of catastrophic forgetting when learning from a data stream using Deep Learning models. Forgetting happens when the model forgets what it learned before while learning a new concept after a drift. The goal is to achieve a balance between the ability to acquire new knowledge (plasticity) and the ability to remember past knowledge (stability).

* CL Scenarios
* Stability-plasticity dilemma
* Evaluation metrics
* ⁠CL strategies
* Hands-on with the Avalanche Python library
