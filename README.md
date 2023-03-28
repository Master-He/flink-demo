# 说明
基于flink1.13.6

运行步骤
1. 准备好java8的linux环境
2. 去官网下载好二进制包，然后解压到随便一个目录，比如/opt/module
3. 启动flink: /opt/module/flink-1.13.6/bin/start-cluster.sh
4. 打开IDEA的maven插件进行打包，打包时注意将全部依赖打包进去
5. 提交job，有两种提交方式
    提交方式1. 去localhost:8081页面提交job
    提交方式2. flink run -c "com.github.chapter05.ClickSourceTest" flink-demo-1.0-jar-with-dependencies.jar
    
    
# 阅读顺序
1. org.example.wordcount

    1.1 BatchWordCount
    
    1.2 StreamWordCount
    
2. com.github.chapter05.ClickSourceTest
   com.github.chapter05.ParallelSourceExample
   com.github.chapter05.RescaleTest
   
3. 



