# AWS's Lambda base image: ships the runtime interface client that lets Lambda
# invoke a handler inside a container. A plain python:3.12 image will not work.
FROM public.ecr.aws/lambda/python:3.12

# Dependencies first, so editing the script does not re-install polars on every build.
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

COPY NflAnalyticsDataPull.py ${LAMBDA_TASK_ROOT}/

# This is the tie-in: <module>.<function>, resolved inside LAMBDA_TASK_ROOT.
CMD ["NflAnalyticsDataPull.handler"]
