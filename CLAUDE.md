# Overview

This is a docker container queue and client, where docker containers and inputs are represented as URLs. In local mode,
the worker acts as both a worker agent and API server.

# Bash commands

- just check: Compile typescript code
- just dev: Run the development stack. But check if it's running first, since changes in code autoload, so use docker
  logs to inspect.
- just dev local: run the development stack with the local worker agent. Prefer this over 'just dev' until running final
  tests
- just test: same as 'just test remote': run the tests with in remote mode
- just test local: run the tests with the local worker agent

# Code style

- just fmt: formats all code and config, run after making code changes
- Destructure imports when possible (eg. import { foo } from 'bar')

# Workflow

- Be sure to typecheck when you’re done making a series of code changes
- Prefer running single tests, and not the whole test suite, for performance

# Current feature to work on: mcp server

- We need llms to be able to run (via mcp server) containers defined by maybe a github repo, maybe Dockerfile, scripts,
  and input files, docker run configuration, env vars, etc.
- The mcp server can get streaming logs back, or wait until the job is done.
- Then can iteratively create working functional containers
- focus on getting the local stack (not the remote) working first
- To test:
  - I want you to test this mcp server in a realistic setting: that means creating a claude code session with the mcp
    server and testing described below
    - use this url for the git repo:
      https://github.com/lenhanpham/OpenThermo/commit/a80273ca1417f0cf29eaf1cee3c1eb5d79bfc4df And the prompt is: create
      a container URL that has this repo above running correctly with the software built and able to be executed This is
      not a functional test, rather a test only run manually due to requiring an LLM
  - install the mcp typescript sdk if needed, and any other tools to help or needed for testing and validating the MCP
    server
- the main argument typescript type for submitting a job is an EnqueueJob object
- ask questions if you don't understand
- important! the goal is for this mcp server to create and execute docker containers as the mcp service. the containers
  are submitted to the local queue, executed, job progress followed if needed, job results returned and inspected:
  exitCode, logs, etc to confirm the job configuration is correct from the prompt
