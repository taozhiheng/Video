# Video

------

这个java project基于storm框架实现了以下功能：

> * 直播视频帧抓取(topology)
> * 视频帧分析识别(topology)
> * 一般网络图片识别(DRPC topology)


# 直播视频帧抓取
## 主要组件：
> * KafkaSpout，接受kafka消息请求
> * ResolveBolt，解析json，将相同url消息发至同一GrabBolt
> * GrabBolt，控制抓取子进程：启动，暂停，继续，停止

## 运行：
``` bash
$ bin/storm jar Video.jar com.persist.VideoGrabber grabber_config.json topology_name
```

## 可用配置(grabber_config.json，必选项前有(!)，可选项前有(defaultValue))：
> * (1)urlSpoutParallel，int，KafkaSpout并行度
> * (3)resolveBoltParallel，int， ResolveBolt并行度
> * (3)grabBoltParallel，int，GrabBolt并行度
> * (!)grabLimit，int，抓取子进程数量上限
> * (!)cmd，string，启动抓取子进程的shell命令(注意jar路径和最后的空格)：“java -Djava.ext.dirs=$STORM_HOME/lib/ -cp Video.jar com.persist.GrabThread ”
> * (!)zks，string，kafka consumer的zookeeper地址(如"zk01:2181,zk02:2181")
> * (!)topic，string，抓取视频接收的kafka topic
> * (!)zkRoot，string，kafka消息存放位置的标识
> * (!)id，string，kafka consumer的id
> * (!)zkServers，string[]，zookeeper集群(如["zk01", "zk02"])
> * (!)zkPort，int，zookeeper端口
> * (!)brokerList，string，发送kafka消息的broker list(如"zk01:9092,zk02:9092")
> * (!)sendTopic，string，抓帧后发送图片信息的topic
> * (!)redisHost，string，redis主机名或ip地址
> * (!)redisPort，int，redis端口
> * (!)redisPassword，string，redis密码
> * (1.0)frameRate，double，抓取帧率(如0.5 - 每2s抓一帧)
> * (3)workerNum，int，worker数量，即进程数量

------

# 视频帧分析识别

## 主要组件：
> * KafkaSpout，接受kafka消息请求
> * PictureResultBolt，解析json，从hdfs下载图片，尝试触发PictureCalculateBolt分析识别
> * PictureCalculateBolt，图片数量足够或者超时时，批量分析识别图片，将结果发至PictureNotifierBolt
> * PictureNotifierBolt，通过redis publish分析结果，并将结果发至PictureRecordBolt
> * PictureRecorderBolt，将分析结果写入hbase

## 运行：
``` bash
$ bin/storm jar Video.jar com.persist.VideoAnalyzer analyzer_config.json topology_name
```

## 可用配置(analyzer_config.json，必选项前有(!)，可选项前有(defaultValue))：
> * (!)so，string，图片分析识别的动态连接库XXX.so，使用绝对路径(/x/x/XXX.so)，或者将其放在java.library.path目录下，直接使用库名(XXX)
> * (0.75f)warnValue，float，分析指标，图片分析的数值至在0.0~1.0之间，超过warnValue将被视为违规图片
> * (227)width，int，图片识别标准宽度
> * (227)height，int，图片识别标准高度
> * (!)bufferSize，int，图片批量识别的标准数量
> * (!)duration，long，两次图片识别最大时间间隔，单位是是是毫秒
> * (1)keySpoutParallel，int，KafkaSpout
> * (3)resultBoltParallel，int，PictureResultBolt并行度
> * (3)notifierBoltParallel，int，PictureNotifierBolt并行度
> * (3)recorderBoltParallel，int，PictureRecorderBolt并行度
> * (!)zks，string，kafka consumer的zookeeper地址(如"zk01:2181,zk02:2181")
> * (!)topic，string，抓取视频接收的kafka topic
> * (!)zkRoot，string，kafka消息存放位置的标识
> * (!)id，string，kafka consumer的id
> * (!)zkServers，string[]，zookeeper集群(如["zk01", "zk02"])
> * (!)zkPort，int，zookeeper端口
> * (!)redisHost，string，redis主机域名或ip地址
> * (!)redisPort，int，redis端口
> * (!)redisPassword，string，redis密码
> * (!)hbaseQuorum，string，hbase域名(如"zk01,zk02,zk03")，不能使用ip
> * (!)hbasePort，int，hbase依赖的zookeeper client端口，默认2181
> * (!)hbaseTable，string，记录结果的hbase表
> * (!)hbaseYellowTable，string，记录违规图片结果的hbase表
> * (!)hbaseColumnFamily，string，hbaseTable的columnFamily
> * (!)hbaseColumns，string[]，hbaseColumnFamily的columns
> * (1)workerNum，int，worker数量，即进程数量，强烈建议设为1，否则gpu资源可能不足，图片识别会有问题

------

# 一般网络图片识别
## 主要组件：
> * DRPCSpout，接受drpc客户端消息请求
> * UrlBolt，解析json，将url集平均分发至DownloadBolt
> * DownloadBolt，从url下载图片，尝试触发PredictBolt分析识别
> * PredictBolt，同一个drpc请求内所有图片下载完成后分析识别，将结果发至ReturnBolt
> * ReturnBolt，将结果返回给客户端

## 运行

### 服务端：
``` bash
$ bin/storm jar Video.jar com.persist.ImageCheck check.json topology_name
```

### 客户端：
``` bash
$ java -Djava.ext.dirs=$STORM_HOME/lib/ -cp Video.jar com.persist.ImageQuery zk02 3772 function url-array-file
```
##　可用配置(check.json，必选项前有(!)，可选项前有(defaultValue))：
> * (!)function，string，drpc运行的function名称，客户端调用时需一致
> * (!)so，string，图片分析识别的动态连接库XXX.so，使用绝对路径(/x/x/XXX.so)，或者将其放在java.library.path目录下，直接使用库名(XXX)
> * (0.75f)warnValue，float，分析指标，图片分析的数值至在0.0~1.0之间，超过warnValue将被视为违规图片
> * (227)width，int，图片识别标准宽度
> * (227)height，int，图片识别标准高度
> * (1)drpcSpoutParallel，int，DRPCSpout并行度
> * (1)urlBoltParallel，int，UrlBolt并行度
> * (5)downloadBoltParallel，int，DownloadBolt并行度
> * (3)returnBoltParallel，int，ReturnBolt并行度
> * (1)workerNum，int，worker数量，即进程数量，强烈建议设为1，否则gpu资源可能不足，图片识别会有问题

### url-array-file示例：
```
[
"http://h.hiphotos.baidu.com/image/h%3D200/sign=c1f5d7eab11c8701c9b6b5e6177e9e6e/8644ebf81a4c510f367f8b146259252dd42aa501.jpg",
"http://h.hiphotos.baidu.com/image/h%3D200/sign=4a4ef5cf94ef76c6cfd2fc2bad17fdf6/f9dcd100baa1cd11dd1855cebd12c8fcc2ce2db5.jpg",
"http://f.hiphotos.baidu.com/image/pic/item/060828381f30e924bd73bbdf48086e061c95f70c.jpg",
"http://e.hiphotos.baidu.com/image/pic/item/f703738da97739120f92f746fd198618367ae265.jpg",
"http://c.hiphotos.baidu.com/image/pic/item/472309f79052982280874be4d2ca7bcb0a46d465.jpg",
"http://e.hiphotos.baidu.com/image/pic/item/91529822720e0cf323633cdc0f46f21fbe09aa05.jpg",
"http://e.hiphotos.baidu.com/image/pic/item/86d6277f9e2f0708e4925827ec24b899a901f206.jpg",
"http://d.hiphotos.baidu.com/image/pic/item/562c11dfa9ec8a13f075f10cf303918fa1ecc0eb.jpg",
"http://d.hiphotos.baidu.com/image/pic/item/cb8065380cd7912344a13298a9345982b3b7809d.jpg",
"http://h.hiphotos.baidu.com/image/pic/item/b7fd5266d0160924ec4504f7d00735fae7cd34fd.jpg",
"http://a.hiphotos.baidu.com/image/pic/item/f9dcd100baa1cd11daf25f19bc12c8fcc3ce2d46.jpg",
"http://c.hiphotos.baidu.com/image/pic/item/a044ad345982b2b782d814fd34adcbef76099b47.jpg",
"http://f.hiphotos.baidu.com/image/pic/item/242dd42a2834349b7eaf886ccdea15ce37d3beaa.jpg",
"http://e.hiphotos.baidu.com/image/pic/item/2f738bd4b31c87019d17540f237f9e2f0608ffb2.jpg",
"http://f.hiphotos.baidu.com/image/pic/item/29381f30e924b899ec899fd16a061d950b7bf6a6.jpg",
"http://h.hiphotos.baidu.com/image/pic/item/ac4bd11373f08202454e6a3e49fbfbedaa641bd6.jpg",
"http://g.hiphotos.baidu.com/image/pic/item/86d6277f9e2f07086605d700eb24b899a901f2aa.jpg",
"http://f.hiphotos.baidu.com/image/pic/item/9358d109b3de9c82c4f95c8f6e81800a19d84315.jpg",
"http://e.hiphotos.baidu.com/image/pic/item/bd315c6034a85edf09a3dc244b540923dd54758d.jpg",
"http://h.hiphotos.baidu.com/image/pic/item/0dd7912397dda1444a7ddaafb0b7d0a20cf486b3.jpg",
"http://b.hiphotos.baidu.com/image/pic/item/4d086e061d950a7b8349e5c408d162d9f2d3c9b6.jpg",
"http://b.hiphotos.baidu.com/image/pic/item/5ab5c9ea15ce36d36c78283038f33a87e950b1b6.jpg",
"http://h.hiphotos.baidu.com/image/pic/item/2934349b033b5bb57e5588fe34d3d539b600bc34.jpg",
"http://c.hiphotos.baidu.com/image/pic/item/730e0cf3d7ca7bcba8076c2fbc096b63f624a8b6.jpg",
"http://f.hiphotos.baidu.com/image/pic/item/03087bf40ad162d97792c91c13dfa9ec8a13cdb6.jpg",
"http://a.hiphotos.baidu.com/image/pic/item/3801213fb80e7becb458abe12d2eb9389b506b34.jpg",
"http://c.hiphotos.baidu.com/image/pic/item/f31fbe096b63f62411fcc7078544ebf81a4ca315.jpg",
"http://g.hiphotos.baidu.com/image/pic/item/0df431adcbef7609dd8c709b2cdda3cc7cd99e2b.jpg",
"http://e.hiphotos.baidu.com/image/pic/item/83025aafa40f4bfb27bfbf2b014f78f0f7361865.jpg",
"http://d.hiphotos.baidu.com/image/pic/item/9922720e0cf3d7ca50f31e09f01fbe096b63a943.jpg",
"http://e.hiphotos.baidu.com/image/pic/item/8b13632762d0f7031db73fdc0afa513d2697c5ad.jpg",
"http://e.hiphotos.baidu.com/image/pic/item/0df431adcbef7609c7e54a962cdda3cc7dd99ec9.jpg",
"http://c.hiphotos.baidu.com/image/pic/item/0b7b02087bf40ad128102ae7552c11dfa9ecce3a.jpg"
]
```

# 项目依赖
> * apache-storm-0.9.6
> * hadoop-2.6.4
> * hbase-1.1.5
> * kafka_2.11-0.10.0.0
> * zookeeper-3.4.8

# 注意
> * 必须配置STORM_HOME环境变量，否则java运行时找不到路径
> * 集群中必须保证启动视频抓取子进程的命令相同(storm库、jar包路径相同:绝对路径相同，或者位于同一环境变量下，如STORM_HOME)
> * 提交topology不能同名，提交drpc topology时先启动$ bin/storm drpc
> * 运行有关图片识别的topology,必须在运行目录下有model.config，用于配置gpu。model.config像这样：
```
model_file: deploy.prototxt
trained_file: _iter_100000.caffemodel
batch_size: 1024
gpu_id: 1
```
> * 每个视频抓取子进程运行时会写日志文件，进程运行时，除了查看、不要对日志文件做其他操作(复制、移动、删除等)

