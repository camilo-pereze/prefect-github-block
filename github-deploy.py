from prefect.deployments import Deployment
from prefect.filesystems import GitHub

github_block = GitHub.load("github-block")

github_dep = Deployment.build_from_flow(
    flow=parent_flow,
    name="github-flow",
    storage = github_block
)

if __name__ == "__main__":
    github_dep.apply()