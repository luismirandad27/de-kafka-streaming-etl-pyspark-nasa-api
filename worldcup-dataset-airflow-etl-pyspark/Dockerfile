FROM quay.io/astronomer/astro-runtime:7.0.0
ENV JAVA_HOME /usr/lib/jvm/java-1.11.0-openjdk-amd64
ENV AWS_ACCESS_KEY  'AKIARFFOJFNA4OOVZ3VD'
ENV AWS_SECRET_ACCESS_KEY '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'
RUN rm -rf /app/*
COPY /app/ /app
RUN rm -rf /include/*
COPY /include/ /include
USER root
RUN chmod -R 777 /app