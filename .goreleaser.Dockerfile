FROM gcr.io/distroless/static-debian13:nonroot
COPY gocast /usr/local/bin/
COPY LICENSE README.md /etc/gocast/
COPY config.json config.schema.json /etc/gocast/
ENTRYPOINT [ "/usr/local/bin/gocast" ]
