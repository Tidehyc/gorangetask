#!/usr/bin/env bash
# GoRangeTask 本地开发用 MySQL 8.0 容器管理脚本
# 目的：用干净、隔离的 MySQL 做本地开发与集成测试，不污染宿主机环境。
# 用法: ./scripts/dev-mysql.sh {up|down|reset|status|shell}
set -euo pipefail

CONTAINER=gotask-mysql
VOLUME=gotask-mysql-data
IMAGE=mysql:8.0
PORT=3306
ROOT_PW=rootpw
DB=gotask

case "${1:-status}" in
  up)
    if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
      echo "容器 $CONTAINER 已存在，直接启动"
      docker start "$CONTAINER"
    else
      docker volume create "$VOLUME" >/dev/null
      docker run -d --name "$CONTAINER" \
        -e MYSQL_ROOT_PASSWORD="$ROOT_PW" \
        -e MYSQL_DATABASE="$DB" \
        -e TZ=UTC \
        -p "127.0.0.1:${PORT}:3306" \
        -v "${VOLUME}:/var/lib/mysql" \
        "$IMAGE" \
        --character-set-server=utf8mb4 \
        --collation-server=utf8mb4_0900_ai_ci \
        --default-time-zone=+00:00 \
        --transaction-isolation=READ-COMMITTED
    fi
    echo -n "等待 MySQL 就绪"
    for _ in $(seq 1 30); do
      if docker exec "$CONTAINER" mysqladmin ping -h127.0.0.1 -uroot -p"$ROOT_PW" --silent >/dev/null 2>&1; then
        echo " -> 就绪"; exit 0
      fi
      echo -n "."; sleep 2
    done
    echo " -> 超时"; exit 1
    ;;
  down)
    docker stop "$CONTAINER" >/dev/null && echo "已停止 $CONTAINER（数据保留在卷 $VOLUME）"
    ;;
  reset)
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    docker volume rm "$VOLUME" >/dev/null 2>&1 || true
    echo "已删除容器与数据卷，如需重建请执行: $0 up"
    ;;
  shell)
    exec docker exec -it "$CONTAINER" mysql -uroot -p"$ROOT_PW" --protocol=TCP "$DB"
    ;;
  status)
    docker ps -a --filter "name=$CONTAINER" --format '{{.Names}}\t{{.Status}}\t{{.Ports}}'
    ;;
  *)
    echo "用法: $0 {up|down|reset|status|shell}" >&2; exit 2
    ;;
esac
