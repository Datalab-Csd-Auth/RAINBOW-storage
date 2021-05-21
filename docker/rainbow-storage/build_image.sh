echo "Getting pom version"
ignite=$(grep version ../../pom.xml | head -n 2 | tail -n 1 | sed -E 's/<.{0,1}version>//g' | awk '{print $1}')
echo "Removing old jars"
mkdir assets
rm assets/*.jar
echo "Building the fat jar"
(cd ../../ ; mvn clean compile assembly:single)
cp ../../target/Ignite-Server-"$ignite"-jar-with-dependencies.jar assets/ignite.jar
echo "Building the Ignite server image"
docker build -t rainbow-storage:"$ignite" .
