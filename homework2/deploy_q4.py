from prefect.deployments import Deployment
from prefect.filesystems import GitHub

github_block = GitHub.load("githubblock")
github_block.get_directory("homework2")

from homework2.parameterized_flow_q1 import etl_parent_flow

github_deploy=Deployment.build_from_flow(
                         flow=etl_parent_flow,
                         name='github_flow')


github_deploy.apply()