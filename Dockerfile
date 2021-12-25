FROM python:3.7-slim-buster

ENV PYTHONPATH "${PYTHONPATH}:/app"
COPY src requirements.txt /app/

RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple/ -r /app/requirements.txt

WORKDIR /app
CMD ["python", "/app/job/market_grab_job.py"]