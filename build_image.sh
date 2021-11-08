echo "Getting pom version"
ignite=$(grep version pom.xml | head -n 2 | tail -n 1 | sed -E 's/<.{0,1}version>//g' | awk '{print $1}')
echo "Building the Ignite server image"
#Testing image
docker buildx build -f Dockerfile_debug --push -t registry.gitlab.com/rainbow-project1/rainbow-integration/rainbow-storage:v."$ignite" --platform  linux/amd64,linux/arm64 .
docker buildx build -f Dockerfile_debug -t rainbow-storage:"$ignite"-test --platform  linux/amd64 .