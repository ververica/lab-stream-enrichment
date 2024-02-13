# Stream Enrichment Lab

This lab demonstrates different ways to enrich data streams in Flink. 

You can specify parameters of your Kafka cluster, your MySQL database via the command line parameters. 
See `StreamEnrichmentJobExercise.main()` on exactly which command line parameters are supported and their default values.
For example, you can specify the following main arguments:

    --bootstrap-servers <broker_1:port,broker_2:port,broker_3:port> --kafka-topic <kafka-topic> \
    --mysql-host <host> --mysql-port <port> --mysql-db <database> --mysql-user <user> --mysql-pass <pass>


This project is using Gradle. To package your job for submission to Flink, use: 

    ./gradlew clean shadowJar 

Afterward, you'll find the jar to use in the [`build/libs`](build/libs) folder.

### Lab Environment Setup

The lab environment has been set up for your training in advance by the trainer. In case you want to set up a similar
environment at home, see the instructions [here](SETUP.md).


### Exercises & Solutions

The detailed exercise instructions and solutions will be provided by your trainer during the training. 
