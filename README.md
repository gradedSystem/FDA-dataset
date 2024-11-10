# FDA open source project to provide the data from **2015 - Present**
![GitHub last commit](https://img.shields.io/github/last-commit/gradedSystem/FDA-dataset) 
![GitHub repo size](https://img.shields.io/github/repo-size/gradedSystem/FDA-dataset)

This data-engineering project is currently using FDA data from [FDA-food-dataset](https://open.fda.gov/)

- TASKS TO DO: 
    - [x] #1 Create `scripts/parse_data.py` which will be used in for GH actions
    - [x] Create `env` in the settings 
    - [ ] Use AWS S3, GLUE in order to process the data
        - [x] S3 bucket was created and updated with the data
        - [x] Adding GH ACTIONS to update aws folder name yearly
        - [ ] Now need to figure out the script which will be used for AWS GLUE
    - [ ] Host the data either in `VERCEL` or `HEROKU` will see which one will be used for this
    - [ ] Use open-source solution for advanced analytics
    - [ ] Make sure to parse the old data from 2004-2014
