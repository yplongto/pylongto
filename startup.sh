#!/bin/bash
if [ "$ENABLED_MONITOR" = "true" ]; then
  ping -c 1 -W 2 127.0.0.1
  if [ $? -eq 0 ]; then
    sed -i 's#n9e.pylongto.yp:3095#127.0.0.1:17000#g' /etc/categraf/conf/config.toml
  fi
  nohup /usr/bin/categraf -configs=/etc/categraf/conf &
fi

case $1 in
    RUN)
      # 主服务启动入口
#      gunicorn -c gunrun.py run:app --preload
      python run.py
    ;;
    PYLONGT_C_BACKEND_BEAT)
      celery -A celery_app beat -l info
    ;;
    PYLONGTO_C_CONTROL_WORKER)
      celery -A celery_app worker -P eventlet -l info -Q backend:pylongto_task_test1, backend:pylongto_task_test2
    ;;
    *)
    echo "ERROR CMD" && exit 1
esac