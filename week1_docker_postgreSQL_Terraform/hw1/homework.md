# Module 1 Homework: Docker & SQL

In this homework we'll prepare the environment and practice
Docker and SQL

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework. 

When your solution has SQL or shell commands and not code
(e.g. python files) file formad, include them directly in
the README file of your repository.


## Question 1. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?
```
docker run -it --entrypoint bash python:3.12.8 \  pip --version
Output: root@13e2004d3deb:/# pip --version
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```
- 24.3.1 (Check)
- 24.2.1
- 23.3.1
- 23.2.1




## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432 (Check)
- db:5432


##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.

## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile

```
SELECT count(lpep_pickup_datetime) 
FROM public.green_tripdata_2019_10 
where trip_distance <=1 and DATE(lpep_pickup_datetime) >= '2019-10-01' AND DATE(lpep_pickup_datetime) < '2019-11-01' ;
```

2. In between 1 (exclusive) and 3 miles (inclusive),

```
SELECT count(lpep_pickup_datetime) 
FROM public.green_tripdata_2019_10 
where trip_distance >1 and trip_distance <=3 and DATE(lpep_pickup_datetime) >= '2019-10-01' AND DATE(lpep_pickup_datetime) < '2019-11-01' ;
```

3. In between 3 (exclusive) and 7 miles (inclusive),

```
SELECT count(lpep_pickup_datetime) 
FROM public.green_tripdata_2019_10 
where trip_distance >3 and trip_distance <=7 and DATE(lpep_pickup_datetime) >= '2019-10-01' AND DATE(lpep_pickup_datetime) < '2019-11-01' ;
```

4. In between 7 (exclusive) and 10 miles (inclusive),

```
SELECT count(lpep_pickup_datetime) 
FROM public.green_tripdata_2019_10 
where trip_distance >1 and trip_distance <=3 and DATE(lpep_pickup_datetime) >= '2019-10-01' AND DATE(lpep_pickup_datetime) < '2019-11-01' ;
```

5. Over 10 miles 

```
SELECT count(lpep_pickup_datetime) 
FROM public.green_tripdata_2019_10 
where trip_distance >10 and DATE(lpep_pickup_datetime) >= '2019-10-01' AND DATE(lpep_pickup_datetime) < '2019-11-01' ;
```

Answers:

- 104,793;  197,670;  110,612;  27,831;  35,281
- 104,793;  198,924;  109,603;  27,678;  35,189
- 101,056;  201,407;  110,612;  27,831;  35,281
- 101,056;  202,661;  109,603;  27,678;  35,189
- 104,838;  199,013;  109,645;  27,688;  35,202 (Check)


## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 


```
select date(lpep_pickup_datetime) as pickup_date, max(trip_distance) 
from public.green_tripdata_2019_10
group by date(lpep_pickup_datetime)
order by max(trip_distance) desc
limit 1
```
- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31 (Check)


## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.


```
select "PULocationID", "Zone", sum(total_amount) as total_amount_new
from (select a.*, b.*
from public.green_tripdata_2019_10 as a
left join public.taxi_zone_lookup as b
on a."PULocationID" = b."LocationID") as c
where date(lpep_pickup_datetime) = '2019-10-18'
group by 1,2
having sum(total_amount) >=13000
order by sum(total_amount) desc
```


- East Harlem North, East Harlem South, Morningside Heights (Check)
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park


## Question 6. Largest tip

For the passengers picked up in Ocrober 2019 in the zone
name "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.


```
select "DOLocationID", "Zone", sum(tip_amount) as total_tip_amount
from (select a.*, b.*
from public.green_tripdata_2019_10 as a
left join public.taxi_zone_lookup as b
on a."PULocationID" = b."LocationID"
where a."PULocationID"=74) as c
group by 1,2
order by sum(tip_amount) desc
limit 1
```



- Yorkville West
- JFK Airport
- East Harlem North (Check)
- East Harlem South


## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-aprove, terraform destroy
- terraform init, terraform apply -auto-aprove, terraform destroy (Check)
- terraform import, terraform apply -y, terraform rm


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw1

```
docker run -it \
    -e POSTGRES_USER="postgres" \ 
    -e POSTGRES_PASSWORD="postres" \ 
    -e POSTGRES_DB="ny_taxi" \ 
    -v dtc_postgres_volume_local:/var/lib/postgresql/data \ 
    -p 5432:5432 \ 
    —network=pg-network \ 
    —name pg-database \ 
    postgres:17
```
