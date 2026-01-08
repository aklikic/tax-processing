# `runtime.json` 

dashboard is generated from `kalix-sre` repo using this command (only datasourceUid different):
```shell
jsonnet -J vendor --ext-str datasourceUid="prometheus-local" --ext-str dashboardUid="runtimenamespacemetrics" --ext-str dashboardTitle="Runtime Namespaces Metrics" runtime.libsonnet > runtime.json
```

# `service-overview.json` and `service-overview-tab.json`

dashboards are generated from `kalix-console-grafana-sync` repo using this command (only datasourceUid different), e.g:
```shell
jsonnet -J vendor --ext-str datasourceUid="prometheus-local" service-overview-tab.libsonnet > ../src/main/resources/dashboards/service-overview-tab.json
```