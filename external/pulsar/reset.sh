# ADMINURL="--admin-url http://localhost:8181"

pulsar-admin $ADMINURL namespaces create public/crawl
pulsar-admin $ADMINURL namespaces set-clusters -c standalone public/crawl

pulsar-admin $ADMINURL namespaces set-backlog-quota public/crawl  --limit 250G -p producer_request_hold

# delete any subscriptions on the topic
pulsar-admin $ADMINURL topics subscriptions public/crawl/in > /tmp/subscriptions

while read subs; do
  pulsar-admin $ADMINURL topics unsubscribe public/crawl/in -s $subs
done </tmp/subscriptions

pulsar-admin $ADMINURL persistent delete-partitioned-topic public/crawl/in

pulsar-admin $ADMINURL persistent create-partitioned-topic public/crawl/in --partitions 10

pulsar-admin $ADMINURL persistent delete public/crawl/out
