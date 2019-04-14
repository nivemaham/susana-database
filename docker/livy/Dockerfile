FROM openjdk:11-jre-slim
MAINTAINER Nicolas PARIS "https://framagit.org/parisni"

ENV SPARK_USER="spark" 

RUN apt-get update \
 && apt-get install -y locales procps \
 && dpkg-reconfigure -f noninteractive locales \
 && locale-gen C.UTF-8 \
 && /usr/sbin/update-locale LANG=C.UTF-8 \
 && echo "fr_FR.UTF-8 UTF-8" >> /etc/locale.gen \
 && locale-gen \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Users with other locales should set this in their derivative image
ENV LANG fr_FR.UTF-8
ENV LANGUAGE fr_FR:en
ENV LC_ALL fr_FR.UTF-8

RUN apt-get update \
 && apt-get install -y curl unzip \
    python3 python3-setuptools \
 && ln -s /usr/bin/python3 /usr/bin/python \
 && easy_install3 pip py4j \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# http://blog.stuart.axelbrooke.com/python-3-on-spark-return-of-the-pythonhashseed
ENV PYTHONHASHSEED 0
ENV PYTHONIOENCODING UTF-8
ENV PIP_DISABLE_PIP_VERSION_CHECK 1

# JAVA
ENV PATH $PATH:$JAVA_HOME/bin

RUN useradd spark

# SPARK
ENV SPARK_VERSION 2.4.1
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-hadoop2.7
ENV SPARK_HOME /usr/spark-${SPARK_VERSION}
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
ENV PATH $PATH:${SPARK_HOME}/bin
RUN curl -sL --retry 3 \
  "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
  | gunzip \
  | tar x -C /usr/ \
 && mv /usr/$SPARK_PACKAGE $SPARK_HOME \
 && chown -R $SPARK_USER:$SPARK_USER $SPARK_HOME

ADD conf/livy.conf /tmp/livy.conf
# LIVY
ENV LIVY_VERSION 0.6.0
ENV LIVY_PACKAGE apache-livy-${LIVY_VERSION}-incubating-bin
ENV LIVY_HOME /usr/livy-${LIVY_VERSION}
RUN curl -o /tmp/livy.zip --retry 3 \
  "http://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}-incubating/apache-livy-${LIVY_VERSION}-incubating-bin.zip" \
 && unzip /tmp/livy.zip -d /usr/ \
 && rm /tmp/livy.zip \
 && mv /usr/$LIVY_PACKAGE $LIVY_HOME \
 && cp /tmp/livy.conf $LIVY_HOME/conf/ \
 && chown -R $SPARK_USER:$SPARK_USER $LIVY_HOME
USER $SPARK_USER
WORKDIR $LIVY_HOME

CMD bin/livy-server start && tail -F conf/livy.conf
