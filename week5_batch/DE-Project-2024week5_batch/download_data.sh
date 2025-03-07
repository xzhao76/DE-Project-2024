set -e
TAXI_TYPE=$1 #"yellow"
YEAR=$2 #2020


#https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-01.csv.gz
#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"
for MONTH in {10..11}; do
    FMONTH=`printf "%02d" ${MONTH}`

    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    mkdir -p ${LOCAL_PREFIX}
    wget ${URL} -O ${LOCAL_PATH}

done

# for MONTH in {1..12}; do
#     FMONTH=`printf "%02d" ${MONTH}`

#     LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
#     LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
#     LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

#     rm -f ${LOCAL_PREFIX}/*.csv

# done