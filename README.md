
  

### Update the current IL
PUT http://127.0.0.1:8888/Forecasting?job_id=job&IL=x

### Get a list of active jobs
GET http://127.0.0.1:8888/Forecasting
### Get details of job_id job
GET http://127.0.0.1:8888/Forecasting?job_id=job

### Stop and existing job
DELETE http://127.0.0.1:8888/Forecasting?job_id=job

## Additional info
The project includes an already trained lstm model (resulting from I10 activities), based on the TensorFlow keras library, that is integrated into the system under trained models with the name lstm11.h5.

Under tools, there is a Kafka producer able to emulate the behaviour of a Prometheus scraper job (for pushing data on a specific Kafka topic)

 
