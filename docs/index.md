---
layout: home

hero:
  name: compute queues
  text: Private job queues you can add compute to
  tagline: A queue is a URL. A job is a Docker container. Push work from a browser or a backend with no authentication and no setup, then add compute by running one command on any machine you own.
  actions:
    - theme: brand
      text: Quickstart
      link: /quickstart
    - theme: alt
      text: Backend integration
      link: /guide/backend-integration
    - theme: alt
      text: Open the web client
      link: https://container.mtfm.io
    - theme: alt
      text: GitHub
      link: https://github.com/metapages/compute-queues

features:
  - title: Browser Docker client
    details: A web page that configures and runs a container, streams its logs, and hands back its outputs. Embeddable in any site as an iframe.
    link: /guide/overview
  - title: Bring your own compute
    details: One `docker run` on a laptop, workstation, or cluster node joins that machine to your queue. Scale by starting more.
    link: /guide/workers
  - title: Three ways to get results
    details: Poll, subscribe over a websocket, or register a callback. Same job, pick whichever fits your backend.
    link: /guide/backend-integration
  - title: Built for agents
    details: An Agent Skill and an llms.txt so an AI can drive a queue correctly on the first try.
    link: /guide/agent-skill
---

<div class="home-blurb">

## 30 seconds

```sh
# 1. pick any queue name — unguessable means private
QUEUE=my-$(uuidgen | tr 'A-Z' 'a-z')

# 2. add compute: this machine now works that queue
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp \
  metapage/metaframe-docker-worker:0.54.80 run --cpus=2 $QUEUE

# 3. push a job from anywhere
curl -s -X POST https://container.mtfm.io/q/$QUEUE \
  -H 'content-type: application/json' \
  -d '{"definition":{"image":"alpine:3.19.1","command":"sh -c \"echo hi > /outputs/out.txt\""}}'
```

</div>

<style>
.home-blurb {
  max-width: 1152px;
  margin: 0 auto;
  padding: 0 24px 64px;
}
.home-blurb h2 {
  border-top: none;
  margin: 0 0 12px;
  padding-top: 0;
}
</style>
