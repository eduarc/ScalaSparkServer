# ScalaSparkServer

<b>Example Finagle server that exposes RESTful services to run and manage Apache Spark Jobs.</b>

Requirements:
- JDK 1.8
- gradle
- IntelliJ IDEA (Optional)

1) Open the Project with IntellJ, build and run the Main class SparkServerMain.

# RESTful Services

a) <code>localhost:8080/init</code> - Init the Spark server with any configuration parameter accepted by the Spark Configuration (SparkConf).

Example: <code>curl -D - "localhost:8080/init?spark.master=local&spark.app.name=SparkServer"</code>

b) <code>localhost:8080/shutdown</code> - Shutdown the Spark server and consequently any running spark job.

c) <code>localhost:8080/queryjob</code> - Shows the current status of the Spark Job given by the parameter <code>id</code>

Example: <code>curl -D - localhost:8080/queryjob?id=1</code>

d) <code>localhost:8080/killjob</code> - Kills the Spark Job given by the parameter <code>id</code>

Example: <code>curl -D - localhost:8080/killjob?id=1</code>

e) <code>localhost:8080/runjob</code> - Runs the job specified by the unique identified <code>id</code> with the particular parameters passed in the url.

Example: <code>curl -D - "localhost:8080/runjob?id=pi&n=100000000"</code>
