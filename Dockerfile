FROM haifengat/ctp_real_md
COPY *.py /home/
COPY *.yml /home/
COPY *.txt /home/
RUN pip install -r /home/requirements.txt

ENTRYPOINT ["python", "/home/tick_min.py"]
