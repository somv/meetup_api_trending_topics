# Top trending topics on Meetup.com

This is a distributable real time spark streaming application that finds top trending topics for a configured period of time on Meetup's site.

## Getting Started

Change the configuration parameters on the application.conf file located in `<base_dir>/src/main/resources/application.conf` as per the requirements.
The current configuration runs perfectly on a single 4 core machine. However, you can change the batch interval, block interval, window duration, slide interval, checkpoint directory
and many more configuration as per you machine/cluster setup.

### Prerequisites

Since this is a Spark program written in Scala. You should have `sbt`, `scala` and `spark` installed in your system.
For version specific information check `build.sbt` file.

### Running test cases

Go inside the project directory and run this command.
```
sbt test
```

### Building project

Go inside the project directory and run this command.
```
sbt assembly
```

## Deployment

This command is for deploying on local machine, if you want to deploy it to a bigger cluster then set additional parameters like master yarn, num-executors, executor-cores executor-memory, driver-memory and deploy-mode etc.

`topN=50 country=us state=TX city=Austin` are optional, provide if you want any geographical filtering on the trending topics. In case arguments are not provided
default values will be used, i.e. the data will not be filtered.
```
spark-submit --master local[*] --class com.humblefreak.analysis.RealTimeProcessing {project_dir}/target/scala-2.11/ing_case_analysis-assembly-0.1.jar topN=50 country=us state=TX city=Austin
```

## Built With
```Scala```
```Apache Spark```
```Sbt```
```ScalaTest```
```spark-testing-base```
```async-http-client```
```play-json```