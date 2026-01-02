```bash
docker build -t rasel143x/yellow-taxi-ui:v1.0 .
docker push rasel143x/yellow-taxi-ui:v1.0

docker-compose down
docker-compose up --build -d
```