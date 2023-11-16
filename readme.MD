

# Introduction
  

This repository contains code and data for my article "[Dashboarding Neo4j Data in Looker Studio: A Powerful Combination](https://dgg32.medium.com/dashboarding-neo4j-data-in-looker-studio-a-powerful-combination-b917935923e7)". 

1. The Python scripts are for the chatbot.

  

# Prerequisite

Neo4j Desktop or AuraDB

GCP account

UMLS free tier
  

# Run
1. Set up the databases as described in the article. 
  
2. Install the necessary Python libraries (install.txt) and gcloud CLI

3. Fill the necessary API key information in config.yaml

4. Run 
```
ti_bigquery.ipynb
```
to convert graph data to BigQuery

5. Create dashboards in Looker Studio


## Authors

  

*  **Sixing Huang** - *Concept and Coding*

  

## License

  

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
