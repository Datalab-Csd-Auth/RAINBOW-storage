echo "Building the fat jar"
(cd ../../ ; mvn clean compile assembly:single)
cp ../../target/*.jar assets/
echo "Building the Ignite client image"
docker build -t ignite-client:1.0.0 .
