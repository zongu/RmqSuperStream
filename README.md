## [rabbitmq.conf](/RabbitMq/rabbitmq.conf)

* 需要留意stream.advertised_host跟stream.advertised_port必須是宿主本身的設定，不是容器內的設定
* 可以預先設定叢集的資訊，記得要符合容器實際上連線狀態

## [enabled_plugins](/RabbitMq/enabled_plugins)

* 運行super stream需要啟用插件

## [docker-compose.yml](/RabbitMq/docker-compose.yml)

* rmq節點啟動順序要注意
* port的部分不要打架即可
