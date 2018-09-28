FROM openjdk:8-jre-alpine

WORKDIR /home

ENV componentName "EnablerResourceManager"
ENV componentVersion 2.0.2

RUN apk --no-cache add \
	git \
	unzip \
	wget \
	bash \
	&& echo "Downloading $componentName $componentVersion" \
	&& wget "https://jitpack.io/com/github/symbiote-h2020/$componentName/$componentVersion/$componentName-$componentVersion-run.jar"

EXPOSE 8200

CMD java $JAVA_HTTP_PROXY $JAVA_HTTPS_PROXY $JAVA_NON_PROXY_HOSTS -DSPRING_BOOT_WAIT_FOR_SERVICES=symbiote-aam:8080 -jar $(ls *run.jar)