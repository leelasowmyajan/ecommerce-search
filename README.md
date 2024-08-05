
<div align="center">

<h2 align="center">Query Intelligence on E-Commerce Search</h2>

  <p align="center">
    <a href="https://github.com/ShivaHariSonu/search-project"><strong>Explore the docs »</strong></a>
    <br />
    <p> <a href="https://drive.google.com/file/d/1oXwjWGjkRG8nBQHg1G1643Ad9gIkMyMt/view?usp=sharing">View Demo </a> </p>
  </p>
</div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#installation">Installation</a></li>
        <li><a href="#how-to-run">How to run</a></li>
        <li><a href="#code-execution">Code Execution</a></li>
      </ul>
    </li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

This project focuses on enhancing the intelligence of query processing within an e-commerce search environment by utilising Apache Solr and Apache Spark to develop advanced search capabilities. The project aims to refine search accuracy and user experience by implementing features such as query boosting based on user interaction, handling misspellings, searching with related keywords, and employing machine learning techniques (i.e., Learning To Rank algorithm) for building generalizable search systems. The integration of these technologies aims to deliver more relevant search results, thereby improving overall customer satisfaction and engagement on the platform.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Built With

* <a href="https://spark.apache.org/">Apache Spark </a>
* <a href="https://solr.apache.org/"> Apache Solr</a>

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started
To get a local copy up and running, follow these simple steps.
### Installation
1. Clone the repo
   ```sh
   git clone https://github.com/ShivaHariSonu/search-project.git
### How to run
Follow these steps to get your development environment set up:
1. Change to the project directory where docker-compose.yml is located:
   ```sh
   cd search-project/search
2. Start the application using Docker Compose:
   ```sh
   docker-compose up --build
   ```
   Note: This process may take about 20 minutes when run for the first time.
3. Accessing the application:
   - JupyterLab interface: Open [http://127.0.0.1:8888/lab?token=test](http://127.0.0.1:8888/lab?token=test) on web browser to access the JupyterLab interface.
   - Visual Studio Code: Open the VS code and click select kernel and paste the url [http://127.0.0.1:8888/lab?token=test](http://127.0.0.1:8888/lab?token=test) and click on the python3 kernel and run the code.
4. Navigate to `search-nb/search-nb.ipynb`, which is the main notebook. 

### Code Execution

**Note:** The following code block is commented out in our project because its execution is time-intensive. We instead utilize previously generated session data stored in `sessions_data.gz` for efficiency.

```python
from time import perf_counter
session_list = []

# Iterate through the top 100 queries
for query in top_100_queries:
    try:
        sess_id = 0
        session_dfs = []
        t1_start = perf_counter()
        
        # Check if the query has canonical rankings
        if len(canonical_rankings[canonical_rankings['query'] == query]) > 0:
            for i in range(NUM_SESSIONS):
                session_dfs.append(synthesize_session(query, sess_id, baselines))
                sess_id += 1
                
                # Break if a large number of sessions are created
                if sess_id >= 2500:
                    print(f"Created {sess_id} sessions for query '{query}' in {perf_counter() - t1_start:.2f} seconds")
                    break
        else:
            print(f"Query '{query}' not available")
        
        # Combine and sort the session data
        sessions_for_query = pd.concat(session_dfs)
        sessions_for_query = sessions_for_query.sort_values(['sess_id', 'dest_rank'])
        sessions_for_query = sessions_for_query[['sess_id', 'query', 'dest_rank', 'clicked_doc_id', 'clicked']] \
                             .rename(columns={'dest_rank': 'rank'})
        session_list.append(sessions_for_query)
    except KeyboardInterrupt:
        break
    except:
        continue

# Combine all sessions
sessions = pd.concat(session_list)
```
This lets us to progress further in our project without having to regenerate session data each time. 
<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- CONTACT -->
## Contact

Gundeti Shiva Hari - u1460836@utah.edu 
<br/>
Leela Sowmya Jandhyala - u1472955@utah.edu

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- Troubleshoot -->
## Troubleshoot
As the code and the data accesses are intensive, it is prone to kernel crashes. If there are kernel crashes you can start from the section itself, instead of from the start. Before starting from that section, load the first cell where it imports necessary libraries.

There are 4 sections overall

* Boosting Products for a Query
* Searching with the related keywords
* Finding wrongly spelled words and mapping it with correct one
* Finding the weightage values for attributes in a query

If you are starting the kernel from the 4th section (Finding the weightage values for attributes in a query) make sure you load the signals_boosting_collection from the cell of the subsection (Boosting the signals).
```python

products_collection="products"
signals_collection="signals"
signals_boosting_collection="signals_boosting"

#We have the collection for products and signals. 
#Creating the collection for signals_boosting
create_collection(signals_boosting_collection)

print("Now we are aggregating to get signals boosts...")
signals_opts={"zkhost": "search-zk", "collection": signals_collection}
df = spark.read.format("solr").options(**signals_opts).load()
df.createOrReplaceTempView("signals")

# For each query we are storing how many times the document has been clicked.
signals_aggregation_query = """
select q.target as query, c.target as doc, count(c.target) as boost
  from signals c left join signals q on c.query_id = q.query_id
  where c.type = 'click' AND q.type = 'query'
  group by query, doc
  order by boost desc
"""

signals_boosting_opts={"zkhost": "search-zk", "collection": signals_boosting_collection, 
                       "gen_uniq_key": "true", "commit_within": "5000"}

#Writing the Boosted Signal Information to Apache Spark
spark.sql(signals_aggregation_query).write.format("solr").options(**signals_boosting_opts).mode("overwrite").save()
print("Signals Aggregation Completed!")

```


<!-- ACKNOWLEDGMENTS -->
## Acknowledgments
List of resources we found helpful and would like to give credit to!
* https://aipoweredsearch.com/
* https://www.cs.cornell.edu/people/tj/publications/joachims_etal_07a.pdf

<p align="right">(<a href="#readme-top">back to top</a>)</p>



