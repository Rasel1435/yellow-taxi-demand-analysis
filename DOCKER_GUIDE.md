```bash
docker build -t rasel143x/yellow-taxi-ui:v1.0 .
docker push rasel143x/yellow-taxi-ui:v1.0

docker-compose down
docker-compose up --build -d

Stop & Delete Containers:
docker rm -f $(docker ps -aq)

Delete All Images:
docker rmi -f $(docker images -q)

Deep Clean:
docker system prune -a

Run New Container:
docker run <image_name>

sudo systemctl restart docker

docker ps

```
```bash
http://localhost:8000/health

```
### Expected output:

```bash
{"status":"healthy"}

```

### To see real-time logs:

```bash
docker-compose logs -f

```
