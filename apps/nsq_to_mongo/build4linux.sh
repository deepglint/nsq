GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o nsq2mongo.linux  nsq_to_mongo.go
echo "==>nsq2mongo.linux"