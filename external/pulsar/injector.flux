name: "injector"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: false
      file: "crawler-conf.yaml"
      override: true

    - resource: false
      file: "pulsar-conf.yaml"
      override: true

spouts:
  - id: "spout"
    className: "com.digitalpebble.stormcrawler.spout.FileSpout"
    parallelism: 1
    constructorArgs:
      - "."
      - "seeds.txt"
      - true

bolts:
  - id: "status"
    className: "com.digitalpebble.stormcrawler.pulsar.PulsarUpdaterBolt"
    parallelism: 1

streams:
  - from: "spout"
    to: "status"
    grouping:
      type: CUSTOM
      customClass:
        className: "com.digitalpebble.stormcrawler.util.URLStreamGrouping"
        constructorArgs:
          - "byHost"
