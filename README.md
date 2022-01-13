# postgre-pusher
Method to push tables into postgre databases.

* Search recursively on folders and subfolders for '.csv' and '.xlsx' extension.
* normalize and cleans: tables names, columns names, data types
* build up sql queries:
  * if the table already exists in the database: **DELETE** content of table and **INSERT** pushed content.
  * if the table does not already exist: **CREATE** the table and **INSERT** pushed content.

### Requirements
**Libraries:**
* pandas 
* psycopg2

**Folder arrangement:**
<br>your script file must be in a subfolder, at 1 distance from the folder root.
<br>Beside that, you can freely arrange the folder.
<br>The following diagram is an example of folder arrangement.

```
folder   
│
└───scripts folder
│   │   pusher.py
│   │   keys.py
│          
└───tables folder
│   │   table.csv
│   │   tables.xlsx
│   │   ...
│   │
│   └───tables subfolder
│      │   table.csv
│      │   tables.xlsx
│      │   ...
│
│   log.txt
```
