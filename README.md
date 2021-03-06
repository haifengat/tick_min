## 说明
处理期货数据 从tick到min

### build
```bash
# 通过github git push触发 hub.docker自动build
docker pull haifengat/tick_min && docker tag haifengat/ctp_real_md haifengat/tick_min:`date +%Y%m%d` && docker push haifengat/tick_min:`date +%Y%m%d`
```

### 启动
```bash
docker-compose up -d
```

### docker-compose
```yml
version: "3.7"
services:
    tick_min:
        image: haifengat/tick_min
        container_name: tick_min
        restart: always
        environment:
            - TZ=Asia/Shanghai
            # tick文件路径
            - tick_csv_gz_path=/home/tick_csv_gz_path
            # 用于保存分钟数据文件的路径
            - min_csv_gz_path=/home/min_csv_gz_path
        volumes:
            # tick文件路径(宿主)
            - /mnt/future_tick_csv_gz:/home/tick_csv_gz_path
            # 用于保存分钟数据文件的路径(宿主)
            - /mnt/future_min_csv_gz:/home/min_csv_gz_path
```