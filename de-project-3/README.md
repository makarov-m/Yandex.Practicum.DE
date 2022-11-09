# The 3rd project

### Project description
The store system continues to evolve: the development team has added functionality for canceling orders and refunding funds (refunded). This means that the processes in the pipeline need to be updated.
Additionally, at the moment the report on customer retention is being built for a very long time, so it is necessary to calculate the necessary metrics in an additional datamart.
The data is coming via API.

### How to work with the repository
1. Copy the repository to your local machine:
	* `git clone https://github.com/{{ username }}/de-project-3.git`
2. Change to the project directory: 
	* `cd de-project-2`
3. Run docker with a command:

```
docker run -d --rm -p 3000:3000 -p 15432:5432 \
 --name=de-project-sprint-3-server \
 sindb/project-sprint-3:latest
```
 After the container starts, you will have access to:
1. Visual Studio Code
2. Air flow
3. Database
at the URL: `http://localhost:3000`

### Repository structure
1. The `migrations` folder stores the migration files. Migration files must have a `.sql` extension and contain an SQL database update script.
2. The `src` folder contains all the necessary sources:
     * DAG folder contains Airflow DAGs.


