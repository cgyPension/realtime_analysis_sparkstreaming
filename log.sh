#!/bin/bash
nginx_home=/opt/module/nginx/sbin
log_home=/home/cgy/applog/gmall/log
java_home=/opt/module/jdk1.8.0_212

case $1 in
"start")
    # 先开nginx
    echo "在 hadoop102 启动nginx"
    nginx=`ps -ef | awk '/nginx/ && !/awk/ {print $n}'`
    if [[ -z "$nginx" ]] ;then
        $nginx_home/nginx
    else
        echo "nginx 已经启动...."
    fi

    # 启动3个日志服务器
    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "在 $host 启动日志服务器"
        ssh $host "nohup $java_home/bin/java -jar $log_home/gmall-logger-0.0.1-SNAPSHOT.jar 1>$log_home/error.log 2>&1  &"
    done
   ;;
"stop")
    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "在 $host 停止日志服务器"
        log=$(ssh $host "$java_home/bin/jps | awk '/gmall-logger-0.0.1-SNAPSHOT.jar/ {print \$n}'")
        if [[ -n "$log" ]]; then
            ssh $host "$java_home/bin/jps | awk '/gmall-logger-0.0.1-SNAPSHOT.jar/ {print \$1}' | xargs kill -9"
        else
            echo "在 $host 没有启动, 不用杀"
        fi
    done

    echo "在 hadoop102 停止nginx"
    nginx=`ps -ef | awk '/nginx/ && !/awk/ {print $n}'`
    if [[ -n "$nginx" ]] ;then
        $nginx_home/nginx -s stop
    else
        echo "nginx 没有启动"
    fi
   ;;
*)
    echo "启动姿势不对"
    echo "  log.sh start 启动日志采集工程"
    echo "  log.sh stop  停止日志采集工程"
;;
esac