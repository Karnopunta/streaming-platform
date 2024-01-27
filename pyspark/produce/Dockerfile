FROM python:3.11
USER root
RUN adduser --disabled-login muser
RUN pip install kafka-python
ADD produce.py /home/muser
RUN chmod a+x /home/muser/produce.py
USER muser
ENTRYPOINT ["/home/muser/produce.py"]