ADMINURL="--admin-url http://localhost:8181"

pulsar-admin $ADMINURL namespaces create public/crawl
pulsar-admin $ADMINURL namespaces set-clusters -c standalone public/crawl

pulsar-admin $ADMINURL namespaces set-backlog-quota public/crawl  --limit 250G -p producer_request_hold

pulsar-admin $ADMINURL persistent delete-partitioned-topic public/crawl/in
pulsar-admin $ADMINURL persistent delete public/crawl/out

pulsar-admin $ADMINURL persistent create-partitioned-topic public/crawl/in --partitions 10
