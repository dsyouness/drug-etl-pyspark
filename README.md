# Pyspark Data Pipeline

## Drug Journal

The objective is to build a data pipeline to process the data defined in the data ressources in order to generate the result with two goals:


>**First** Data pipeline must output a JSON file that represents a link graph between the different drugs and their respective mentions in the different PubMed publications, the different scientific publications and finally the newspapers with the date associated with each of these mentions. The representation below shows what is expected. There can be several ways to model this output and you can justify your vision:
>- A drug is considered mentioned in a PubMed article or a clinical trial if it is mentioned in the title of the publication.
>- A drug is considered to be mentioned by a newspaper if it is mentioned in a publication issued by this newspaper.


>**Second** Set up (outside the data pipeline, you can consider that it is an additional part) a feature allowing to answer the following problem:
>- Extract from the json produced by the data pipeline the name of the journal that mentions the most different drugs

##  Environment

- Python 3
- Pyspark

## Project structure

```python
├── data
│   ├── sink
│   │   ├── drug_graph.json
│   │   └── top_journals.json
│   └── source
├── main.py
├── setup.py
├── sql # SQL part
├── src
│   ├── configs # ETL config
│   ├── dependencies
│   │   ├── logging.py
│   │   └── spark.py
│   ├── io
│   │   └── source.py
│   ├── publication
│   │   ├── job.py
│   │   └── publication.py
│   ├── schema
│   │   └── schemas.py
│   └── utils
│       ├── constants.py
│       └── utils.py
└── tests
    └── test_source.py
```

## Launch job

### Command line
```python
# to launch job for grug graph
python main.py drug_graph

# to launch job top_journals
python main.py top_journals
```
### Airflow

```python
# use the SparkSubmitOperator in Airflow DAG
spark_submit_local = SparkSubmitOperator(
		application = 'main.py' ,
		conn_id= 'spark_local', 
		task_id='spark_submit_task', 
		dag=dag_spark
		)
```

## Test

```python
# Execute all tests:
python -m unittest discover

# Execute specific test:
python -m unittest tests.test_source.TestSource
```

## SQL

All the sql queries are in the **sql** directory :
- Queries for part 1
- Queries for part 2