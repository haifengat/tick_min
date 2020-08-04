FROM haifengat/ctp_real_md
COPY *.py /home/
COPY *.yml /home/
COPY *.txt /home/
RUN pip install -r /home/requirements.txt
ENV tick_csv_gz_path /home/tick_csv_gz_path
ENV min_csv_gz_path /home/min_csv_gz_path

ENTRYPOINT ["python", "/home/tick_min.py"]
