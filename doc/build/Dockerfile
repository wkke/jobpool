FROM registry.cn-beijing.aliyuncs.com/yunli-data-platform/alpine:3.6

RUN addgroup jobpool && \
    adduser -S -G jobpool jobpool

ENV GLIBC_VERSION "2.25-r0"

ADD gosu-amd64 /usr/local/bin/gosu

RUN set -x && \
    apk --update add --no-cache --virtual .gosu-deps tzdata dpkg curl ca-certificates gnupg libcap openssl && \
    curl -Ls https://github.com/andyshinn/alpine-pkg-glibc/releases/download/${GLIBC_VERSION}/glibc-${GLIBC_VERSION}.apk > /tmp/glibc-${GLIBC_VERSION}.apk && \
    apk add --allow-untrusted /tmp/glibc-${GLIBC_VERSION}.apk && \
    rm -rf /tmp/glibc-${GLIBC_VERSION}.apk /var/cache/apk/* && \
    export GNUPGHOME="$(mktemp -d)" && \
    chmod +x /usr/local/bin/gosu && \
    gosu nobody true && \
    apk del .gosu-deps

RUN apk add -U tzdata
RUN cp  /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo "Asia/shanghai" >> /etc/timezone

COPY jobpool /bin/jobpool

RUN mkdir -p /jobpool/data && \
    mkdir -p /etc/jobpool && \
    chown -R jobpool:jobpool /jobpool

EXPOSE 4646 4647 4648

ADD start.sh /usr/local/bin/start.sh

ENTRYPOINT ["/usr/local/bin/start.sh"]
