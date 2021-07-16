#!/bin/bash

readConf () {
    echo $(cat app/settings.cfg | grep $1 | awk -F"=" '{ print $2 }')
}

DOCKER_CONTAINER=$(readConf "hive_server_container")
HDFS_USER=$(readConf "hdfs_user")

HDFS_PATH="/user/${HDFS_USER}/painel_hist_covidbr"

echo -ne "Checando container $DOCKER_CONTAINER "
if [ ! $(docker ps -q -f name="$DOCKER_CONTAINER") ]; then
    echo "Docker container not found: $DOCKER_CONTAINER"
    exit 127
fi
echo -e '\033[0;32mOK\033[0m'

echo -ne "Copiando arquivos para o container... "
for file in input/{*.csv,*.sql}; do
    docker cp $file "$DOCKER_CONTAINER:/tmp"
done
echo -e '\033[0;32mOK\033[0m'

echo -ne "Carregando arquivos para o HDFS... "
docker exec $DOCKER_CONTAINER bash -c 'hdfs dfs -mkdir -p '$HDFS_PATH
docker exec $DOCKER_CONTAINER bash -c 'hdfs dfs -moveFromLocal -f /tmp/*.csv '$HDFS_PATH'/'
echo -e '\033[0;32mOK\033[0m'

echo "Criando tabela Hive particionada... "
# Por alguma razão, beeline nao reconhece variaveis declaradas por "hivevar". Usando hive-cli como alternativa
docker exec $DOCKER_CONTAINER bash -c 'hive jdbc:hive2:// -f /tmp/hive_script.sql --hivevar HDFS_PATH="'$HDFS_PATH'"'

echo -e '\033[0;32mDONE\033[0m'
