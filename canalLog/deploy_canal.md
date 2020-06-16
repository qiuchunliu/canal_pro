服务用途：    

​		通过监控目标库的binlog来捕捉数据变动
部署准备：    

​		1、附件即为部署所需文件，解压后稍作配置即可    

​		2、藏信数据库准备：      

​				#配置mysql的my.cnf ，开启binlog、置为ROW模式    

```shell
[mysqld]        
log-bin=mysql-bin        
binlog-format=ROW        
server_id=1
```

​				#创建mysql的canal用户：user=canal_for_5veda；password=pwdfor5veda。并赋予权限        

```mysql
CREATE USER canal_for_5veda IDENTIFIED BY 'pwdfor5veda';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal_for_5veda'@'%'; 
FLUSH PRIVILEGES;
```

​		3、解压压缩包后得到如下目录        

```shell
bin
conf
lib
logs
runjob
```

​		4、进入conf目录，修改 canal.properties        

```shell
#tcp bind ip        
canal.ip = canal部署机的内网IP 
```

​		 5、进入conf/klingon2_etl目录，修改 instance.properties  

```shell
#position info        
canal.instance.master.address=藏信数据库IP:PORT        
#username/password        
canal.instance.dbUsername=canal_for_5veda        
canal.instance.dbPassword=pwdfor5veda 
```

​		6、进入 bin 目录，启动服务        

```shell
./startup.sh
```

​    	7、进入runjob目录，命令行执行命令启动cdc程序

```shell
java -DcanalUrl=canalIP:11121/klingon2_etl -DmysqlConnStr='mysql|base159=user:pwd@目标库IP:PORT/defaultBase' -DbatchSize=20000 -DschemaPath=absolute_path_of_etl_init.xml -DsleepDur=1000 -jar canon.jar
```