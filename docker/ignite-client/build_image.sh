echo "Getting pom version"
ignite=$(grep version ../../pom.xml | head -n 2 | tail -n 1 | sed -E 's/<.{0,1}version>//g' | awk '{print $1}')
echo "Building the fat jar"
(cd ../../ ; mvn clean compile assembly:single)
cp ../../target/*.jar assets/
echo "Building the Ignite client image"
docker build -t ignite-client:"$ignite" .
