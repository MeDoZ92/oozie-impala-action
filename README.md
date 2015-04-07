### Oozie Impala Action

This borrows heavily from [Mark Brook’s excellenet work](https://github.com/onefoursix/Cloudera-Impala-JDBC-Example).

#### Usage Instruction

##### CLI
To use on the command line, do the following -

```sh
$ git clone <>
$ cd <>
$ mvn clean package
$ cat >sample.sql <<EOF
SELECT 1+1
EOF
$ export CONNECTION_URL="jdbc:hive2://impalad_host:impalad_port/default;auth=noSasl"
$ java -jar target/oozie-impala-action-1.0-jar-with-dependencies.jar sample.sql "$CONNECTION_URL"
```

##### Oozie
Refer to the example in `run-oozie/workflow.xml`
