# The 1st project 

### Description
The idea of that project is to create RFM (Recency, Frequency, Monetary Value) segmentation of the of application users.

### ETL from external API to local database

![](pics/yandexpracticum_de_project_4.png)


### How to work with the repository

1. Copy the repository to your local machine:
	* `git clone https://github.com/{{ username }}/de-project-sprint-5.git`
2. Change to the project directory: 
	* `cd de-project-sprint-5`
3. Run docker-compose:
```
docker-compose up -d
```
4. After the container starts, you will have access to:
- Airflow
	- `localhost:3000/airflow`
- Database
	- `jovyan:jovyan@localhost:15432/de`


### Repository structure
- `/src/dags`
- `/pics/`
