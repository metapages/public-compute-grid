# Deploy to Fly

This is a quick-start with the steps needed to deploy metapage's workers to Fly.io. For a full walkthrough that explains
things in more detail, see the
[Deployment on Fly.io](https://www.notion.so/metapages/fly-io-d9ea8c75141c42aeba5c93f3f7143b48) published doc.

## Prerequisites

- [Fly.io account](https://fly.io/), and the [Fly CLI](https://fly.io/docs/getting-started/installing-flyctl/)
- A couple UUIDs to serve as worker queue IDs later. I just use `uuid`/`uuidgen` in the shell for this.

## Steps

Fill out details of the worker app TOML files. In particular make sure they have the SAME prefix for their names, but
that their names are globally unique (no other Fly app can have the same name). Adjust machine specs, etc.

For each set of workers, create an empty app and stage a secret containing the unique queue ID. Then, deploy the app.

```sh
fly apps create metapage-io-workers-a
fly secrets set -a metapage-io-workers-a METAPAGE_IO_WORKER_QUEUE_ID=[generatedID]
fly deploy -c workers-a.fly.toml --yes --ha=false
```

If you want more than 1 worker available, you can scale the app up manually now, but we should set up autoscaling first.
Populate the `METRICS_TARGETS` environment variable in `metrics-proxy.toml` with the metapage job queue metrics
endpoints for each worker app's queue, and the corresponding name of the worker app in fly. Then deploy the metrics
proxy:

```sh
fly launch -c metrics-proxy.toml --copy-config --yes --ha=false
```

Check Fly.io's provided metrics dashboard to see if the metrics are being collected correctly. You should see a metric
`queue_length`, which can be filtered by the `app_name` label to see the queue length of each worker app.

If needed, modify the contents of `autoscaler.fly.toml` to adjust the `FAS_APP_NAME` environment variable. This is used
to target the apps that will be autoscaled, and it should be a wildcard that matches the prefix of the worker apps
you've launched above. We'll make an empty app again, create an org-wide deploy token that gives the autoscaler
permission to scale apps and read from Prometheus, and deploy the autoscaler:

```sh
fly apps create metapage-io-autoscaler
autoscaler_prometheus_token=$(fly tokens create readonly)
fly secrets set -a metapage-io-autoscaler --stage FAS_PROMETHEUS_TOKEN=$autoscaler_prometheus_token
autoscaler_api_token=$(fly tokens create org)
fly secrets set -a metapage-io-autoscaler --stage FAS_API_TOKEN=$autoscaler_api_token
fly deploy -c autoscaler.fly.toml --ha=false
```

To destroy any apps, just run e.g.:

```sh
fly apps destroy metapage-io-workers-a
```

This will destroy the app and _any_ associated resources.
