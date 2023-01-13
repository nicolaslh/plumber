# plumber
使用workerman消费队列数据

## Installation

You can add this library as a local, per-project dependency to your project using Composer:

``` shell
composer require plumber/plumber
```

## Use

```php
$queuename = "queuename";
$callback = "test::test"; //消费sqs数据会会交由该方法处理
$config = []; //配置信息
$plumber = new plumber($queuename, $callback, $config);
$plumber->run();
```

config配置信息说明
* `provider` - 队列产品, 如SQS, RabbitMQ
* `count` - workerman所起的进程数量
* `rabbitmq` - rabbitmq credentials 集合
  * `host` - 链接地址
  * `port` - 端口
  * `login` - 用户名
  * `password` - 密码
  * `vhost` - vhost地址默认为"`/`"
* `key` - aws account id
* `secret` - aws account secret
* `region` - region

callback函数返回数据格式

```php
["msg" => "success"]
```
注意: <strong>只要当msg为`successs`时, 程序才会从队列中移除已经处理的消息</strong>

## run
```shell
Usage: php yourfile <command> [mode]
Commands: 
start           Start worker in DEBUG mode.
                Use mode -d to start in DAEMON mode.
stop            Stop worker.
                Use mode -g to stop gracefully.
restart         Restart workers.
                Use mode -d to start in DAEMON mode.
                Use mode -g to stop gracefully.
reload          Reload codes.
                Use mode -g to reload gracefully.
status          Get worker status.
                Use mode -d to show live status.
connections     Get worker connections.
```
