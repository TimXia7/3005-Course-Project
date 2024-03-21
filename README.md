# 3005-Course-Project




Exporting the database (FINISH LATER):

- On windows, we had to modify the PATH environment variable, specifically adding the path:
    - C:\Program Files\PostgreSQL\15\bin, or wherever the bin folder is located in your PostgreSQL folder

and then enter in the following command (for us at least) to export it:

pg_dump -U postgres -p 5433 FinalProject > dbexport.pgsql

(-p is optional, my port number differed)

this was done on a windows system, so I did not have access to sudo, instead I ran Windows Powershell as an administrator 


Python Script to Parse Data:

- Using the competitions.json file, I found that the match files I needed were in subfolder 11 for La Liga, and 2 for Premier League. 

- I needed the season_ids 90, 42, and 4 in subfolder 11, and season_id 44 in subfolder 2.

- As a result, I got my match data from 90.json, 42.json, 4.json, and 44.json

