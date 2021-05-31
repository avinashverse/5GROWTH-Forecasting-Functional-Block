# 5gr-FFB

### Project information
5GROWTH is funded by the European Union’s Research and Innovation Programme Horizon 2020 under Grant Agreement no. 856709


Call: H2020-ICT-2019. Topic: ICT-19-2019. Type of action: RIA. Duration: 30 Months. Start date: 1/6/2019


<p align="center">
<img src="https://upload.wikimedia.org/wikipedia/commons/b/b7/Flag_of_Europe.svg" width="100px" />
</p>

<p align="center">
<img src="https://5g-ppp.eu/wp-content/uploads/2019/06/5Growth_rgb_horizontal.png" width="300px" />
</p>
 
# 5GROWTH-Forecasting Functional Block (5Gr-FBB)
This repository contains the code for the Forecasting Functional Block developed in the 5Growth EU project.

The scope of this block is to perform the forecasting of specific parameter(s) in order to be avalable at the 5Growth stack for taking decisions (i.e., scaling of a NS).

This project is based on Python3.8 interpreter.

## Requirements
All the python project dependencies are stored in the file requirements38.txt and so you can install them by running the following command:

    pip install -r requirements38.txt
    
## Installation
Copy the projet folder in the dedicate machine.

Edit the configuration file of the project, by setting the parameters (i.e., IP, port and url) for the 4 main modules of the 5Growth stack to interact with.


## Usage
 
Run the command `python3 5grfbbAPI.py`.
The project actvates a flask rest server, listening by default on port 8888.

The main APIs are described following:
### create a new job
POST http://127.0.0.1:8888/Forecasting
   
   { 
    "nsId" : "fgt-82f4710-3d04-429a-8243-5a2ac741fd4d",
    "vnfdId" : "spr2",
    "performanceMetric" :  "VcpuUsageMean",
    "nsdId" : nsEVS_aiml,
    "IL" : 1
   }

### Update the current IL
PUT http://127.0.0.1:8888/Forecasting?job_id=job&IL=x

### Get list of active jobs
GET http://127.0.0.1:8888/Forecasting
### Get details of job_id job
GET http://127.0.0.1:8888/Forecasting?job_id=job

### Stop and exeisting job
DELETE http://127.0.0.1:8888/Forecasting?job_id=job

## Additional info
The project includes an already trained lstm model (resulting from I10 activities), based on the tensorflow keras library, that is integrated in the system, under trainedModels with name lstm11.h5.

Under tools there is a kafka producer able to emulare the behaviour of a prometheus scraper job (for pushing data on a specific Kafka topic)

 
