all:
	mvn clean package
	java -jar target/Top3Airports-0.1-SNAPSHOT-jar-with-dependencies.jar flights.csv  intermediatefolder  output

clean:
	rm -r intermediatefolder
	rm -r output
