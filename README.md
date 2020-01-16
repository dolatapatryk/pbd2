# pbd2
1. skopiuj te pliki do folderu `projekt` na twoim buckecie: `authorities.jar`, `createTables.sh`, `dates.jar`, `download.sh`, `fillTables.sh`, `hive.hql`, `vehicle-types.jar`, `weathers.jar`
2. skopiuj pliki z bucketu na klaster: `gsutil cp -r gs://<your_bucket_name>/projekt .`
3. `cd projekt`
4. `chmod +x *.sh`
5. `./download.sh`
6. `./createTables.sh`
7. `./fillTables.sh`
