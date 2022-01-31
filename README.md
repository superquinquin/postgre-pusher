# postgre-pusher
Method to push tables from **Google Drive** and **Dropbox** into **postgre databases**.

**Compatible formats**: Excel (.xlsx), CSV and Google Sheets

**setup Drive**: 
* Creating a project/app on google clound (https://console.cloud.google.com/)
* Enabling Google drive api on the library
* Add a service account user (bot) and grant him rights on the google drive api
* create a shared folder whereever you want with the service account by using its email adress
* collect the folder id ( last token of the folder url ) to connect the service account with the shared file.

**setup Dropbox**:
* create a Dropbox app (https://www.dropbox.com/developers)
* grant permissions on files and folder, and maybe collaborations
* on settings page, generate access token and add 'No expiration' for the access token'
* use the token to access dropbox api and the the path of the folder you want to connect

### Process:

```                                                                                
                      │_______________server_______________│
│_______Puller_______││_______________Pusher_______________│                  

├── Google Drive ---│
                    ├── image folder ----> Postgre database
├── Dropbox --------│

```

**Puller methods**:
* Puller Methods searchs recursively in the connected folders. It collect all XLSX, CSV and Google Sheet files
* Google Sheets are transformed as XLSX files while dumped.
* Dump all collected files on a folder in the server with a path defined as **target path**
* you can enable the connection with Google drive thanks to **'enable_drive_pull' == True**, same with **'enable_dbx_pull' == True** for Dropbox
* On Dropbox, only files with a server modified delta lower than one day are dumped. This feature is desabled on drive as there is no such metadata.
* The target file on the server is an image of Drive folder + Dropbox folder. Removing files from the Drive or Dropbox remove them from the server folder.

**Pusher methods**:
* takes files from the server folder Push them into the Postgre database
* adjust the columns dtype and normalize tables and columns names
* If the table already exist on the database, the Query drop old table and recreate a new table. Creating a new table appear to be necessary to handle columns modifications betwenn both versions.
* **'enable_drop' == True** transfert the droping behavior of the folder to the database. When enabled, when a file is removed from Drive or Dropbox, the file will also be removed from the server folder and the database


### Requirements
**Libraries:**
* psycopg2
* openpyxl
* dropbox ( if pull drom dropbox is enabled )
* google-auth and google-api-python-client ( if pull from drive is enabled )
