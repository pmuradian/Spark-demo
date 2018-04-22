Running the application:

.jar executable is located in target folder and is named grdev-1.0-SNAPSHOT.jar

To run the program use: spark-submit --class "SparkDemo" --master local target/grdev-1.0-SNAPSHOT.jar
Pass no command line arguments to jar to run numbers and bags demos, pass --numbers only for numbers demo, pass --bags for bags demo