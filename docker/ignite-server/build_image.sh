echo "Building the fat jar"
(cd ../../ ; mvn clean compile assembly:single)
cp ../../target/*.jar assets/
echo "Building the Ignite server image"
docker build -t ignite-server:1.0.0 .
