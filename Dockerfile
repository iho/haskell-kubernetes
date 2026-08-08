FROM haskell:9.10.3

WORKDIR /build
COPY kube-hs.cabal ./
RUN cabal update
COPY src ./src
COPY exe ./exe
COPY examples ./examples
RUN cabal build exe:configmap-logger

RUN mkdir -p /out && cp "$(cabal list-bin configmap-logger)" /out/configmap-logger

ENTRYPOINT ["/out/configmap-logger"]
