# The 1st project 

### Project description
The idea of that project is to create RFM (Recency, Frequency, Monetary Value) user segmentation.

- Recency - how much time has passed since the last order. Scale = [1...5]: 5 - is recent, 1 - is far in the past.
- Frequency - the number of orders. Scale = [1...5]: 5 - big amout of orders, 1 - small number of orders.
- Monetary Value - the amount of the client's costs. Scale = [1...5]: 5 - big spend, 1 - small spend.

### Scripts description

	* datamart_ddl.sql - creates datamart table
	* datamart_query.sql - loads data to the datamart
	* orders_view.sql - creates view with orders
	* starting_script.sql - data exploration
	* tmp_rfm_frequency.sql - creates table with frequency values
	* tmp_rfm_monetary_value.sql - creates table with monetary values
	* tmp_rfm_recency.sql - creates table with recency values
	* views.sql - create a necessary views to explore the data

### How to work with the repository

1. Copy the repository to your local machine:
	* `git clone https://github.com/{{ username }}/de-project-1.git`
2. Change to the project directory: 
	* `cd de-project-1`
3. Run docker with a command:

```
docker run -d --rm -p 15432:5432 -p 3000:3000 \
--name=de-project-sprint-1-server-local \
sindb/project-sprint-1:latest 
```

4. After the container starts, you will have access to:
- Database
	- `http://localhost:3000`

### Repository structure
- `/src/`
