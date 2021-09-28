echo "Getting pom version"
ignite=$(grep version pom.xml | head -n 2 | tail -n 1 | sed -E 's/<.{0,1}version>//g' | awk '{print $1}')
echo "Building the Ignite server image"
#Gitlab repos
docker buildx build -t registry.gitlab.com/rainbow-project1/rainbow-integration/rainbow-storage:"$ignite"-arm64 --platform arm64 .
docker buildx build -t registry.gitlab.com/rainbow-project1/rainbow-integration/rainbow-storage:"$ignite"-amd64 --platform amd64 .
docker buildx build -t registry.gitlab.com/rainbow-project1/rainbow-storage:"$ignite"-arm64 --platform arm64 .
docker buildx build -t registry.gitlab.com/rainbow-project1/rainbow-storage:"$ignite"-amd64 --platform amd64 .
#Testing image
docker buildx build -t rainbow-storage:"$ignite"-amd64 --platform amd64 .