# postgre-pusher
Method to push tables into postgre databases.

* Search recursively on folders and subfolders for '.csv' and '.xlsx' extension.
* normalize and cleans: tables names, columns names, data types
* build up sql queries:
  * if the table already exists in the database: **DROP** table, **CREATE** new table and **INSERT** pushed content. (drop is needed to handle table columns changes)
  * if the table does not already exist: **CREATE** the table and **INSERT** pushed content.

### Requirements
**Libraries:**
* pandas 
* psycopg2

**Inputs:**
* **Root folder path** as init path variable
* **postgre connection informations**
