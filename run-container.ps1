# Stop and remove existing container if it exists
docker stop sommscore 2>$null
docker rm sommscore 2>$null

# Run the container
docker run -d `
    --name sommscore `
    -p 8000:8000 `
    -v "${PWD}/data:/data" `
    -e "DB_PATH=/data/commerce7.db" `
    -e "C7_TENANT=donum" `
    -e "C7_AUTH_TOKEN=ZG9udW0tZGF0YS1wdWxsOjM3TDJrMHA2MXY3aGIzUk90NGdNOTVDSjkyY1NEa3FrVGpCOTRvOEg3bGp2MWRVQTczM2RoRUY5Q0JjMmVWM1E=" `
    -e "FLASK_ENV=production" `
    -e "FLASK_APP=app.py" `
    sommscore

# Show container logs
docker logs sommscore 