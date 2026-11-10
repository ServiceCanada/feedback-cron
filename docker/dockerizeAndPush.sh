#!/bin/bash
cd ..
mvn package
docker build . -f ./docker/Dockerfile -t tbsacr.azurecr.io/pagefeedback-cj:1.0.0
docker push tbsacr.azurecr.io/pagefeedback-cj:1.0.0
