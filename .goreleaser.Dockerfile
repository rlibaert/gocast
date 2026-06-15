FROM gcr.io/distroless/base-debian13:nonroot
COPY gocast /usr/local/bin/
ENTRYPOINT [ "/usr/local/bin/gocast" ]
