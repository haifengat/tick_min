## 说明
处理期货数据 从tick到min


#### DockerFile
```dockerfile
FROM haifengat/ctp_real_md
COPY *.py /home/
COPY *.yml /home/
COPY *.txt /home/
RUN pip install -r /home/requirements.txt

ENTRYPOINT ["python", "/home/tick_min.py"]

```

### build
```bash
# 通过github git push触发 hub.docker自动build
docker pull haifengat/ctp_real_md && docker tag haifengat/ctp_real_md haifengat/ctp_real_md:`date +%Y%m%d` && docker push haifengat/ctp_real_md:`date +%Y%m%d`
```

### docker-compose
```yml
version: "3.1"

services:
    tick_min:
        image: haifengat/tick_min
        container_name: tick_min
        restart: always
        environment:
            - TZ=Asia/Shanghai
            - tick_csv_gz_path=/home/tick_csv_gz_path
            - min_csv_gz_path=/home/min_csv_gz_path
        volumes:
            - /mnt/future_csv_gz:/home/tick_csv_gz_path
            - /mnt/future_min_csv_gz:/home/min_csv_gz_path
            
```