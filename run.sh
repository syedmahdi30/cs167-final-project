mvn clean package

beast --conf spark.executor.memory=32g --conf spark.driver.memory=32g --class edu.ucr.cs.cs167.master.TaskA1 target/master_project-1.0-SNAPSHOT.jar Chicago_Crimes_1k.csv.bz2 Chicago_Crimes_ZIP.parquet
beast --conf spark.executor.memory=32g --conf spark.driver.memory=32g --class edu.ucr.cs.cs167.master.TaskA2 target/master_project-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP.parquet ZIPCodeCrimeCount
beast --conf spark.executor.memory=32g --conf spark.driver.memory=32g --class edu.ucr.cs.cs167.master.TaskA3 target/master_project-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP.parquet 03/15/2018 03/15/2019
beast --conf spark.executor.memory=32g --conf spark.driver.memory=32g --class edu.ucr.cs.cs167.master.TaskA4 target/master_project-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP.parquet 01/01/2000 01/01/2010 -87.6400 41.8700 -87.6100 41.9000
beast --conf spark.executor.memory=32g --conf spark.driver.memory=32g --class edu.ucr.cs.cs167.master.TaskA5 target/master_project-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP.parquet

