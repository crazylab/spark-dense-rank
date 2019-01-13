Create a fat jar by running the following command from the project root directory
```bash
sbt clean compile assembly
```

The above command will create a jar file under `./target/scala-{x.x}/{project-name}-assembly-{version}-SNAPSHOT.jar`.

The job can be submitted using the following command
```bash
spark-submit --class com.example.Main --master local[*] ./target/scala-2.12/scala-spark-project-assembly-0.1.0-SNAPSHOT.jar
```