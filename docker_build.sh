set -ex
docker build -t buildkite_gcp_scaler .
docker tag buildkite_gcp_scaler artifactory.internal.bosdyn.com/docker-bosdyn/buildkite-gcp-autoscaler
docker push artifactory.internal.bosdyn.com/docker-bosdyn/buildkite-gcp-autoscaler
