
javac $(find src -name '*.java')

java -cp "src:jars/*" com.k2view.x12mask.Main stage application.properties

java -cp "src:jars/*" com.k2view.x12mask.Main mask TXN-1002 application.properties
java -cp "src:jars/*" com.k2view.x12mask.Main mask TXN-1003 application.properties

java -cp "src:jars/*" com.k2view.x12mask.Main clear