/**
 * Licensed to DigitalPebble Ltd under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. DigitalPebble licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.digitalpebble.stormcrawler.pulsar;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;

public class PulsarUpdaterBolt extends AbstractStatusUpdaterBolt {

    Producer<String> producer;
    PulsarClient client;

    ConcurrentHashMap<String, Tuple> unacked = new ConcurrentHashMap<>();

    private URLPartitioner partitioner;
    private boolean doRouting = true;

    private static final Logger LOG = LoggerFactory
            .getLogger(PulsarUpdaterBolt.class);

    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        String service = ConfUtils.getString(stormConf,
                "pulsar.status.serviceUrl", "pulsar://localhost:6650");
        String topic = ConfUtils.getString(stormConf, "pulsar.status.topic");
        String producerName = ConfUtils.getString(stormConf,
                "pulsar.status.producerName", context.getStormId());
        int totalTasks = context
                .getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks > 1) {
            producerName += "_" + context.getThisTaskIndex();
        }

        // set to infinity (i.e. no timeout)
        int sendTimeout = ConfUtils.getInt(stormConf,
                "pulsar.status.sendTimeout", 0);
        doRouting = ConfUtils.getBoolean(stormConf, "pulsar.status.routing",
                doRouting);

        try {
            client = PulsarClient.builder().serviceUrl(service).build();
            ProducerBuilder<String> producerBuilder = client
                    .newProducer(Schema.STRING).enableBatching(true)
                    .blockIfQueueFull(true).batchingMaxMessages(500)
                    .topic(topic).sendTimeout(sendTimeout, TimeUnit.SECONDS);
            if (producerName != null) {
                producerBuilder.producerName(producerName);
            }
            producer = producerBuilder.create();
        } catch (Exception e) {
            LOG.error("Exception caught when instantiating updater bolt", e);
            throw new RuntimeException(e);
        }

        if (doRouting) {
            partitioner = new URLPartitioner();
            partitioner.configure(stormConf);
        }
    }

    @Override
    public void cleanup() {
        producer.closeAsync();
        client.closeAsync();
    }

    @Override
    /**
     * Do not ack the tuple straight away! wait to get the confirmation that it has
     * worked
     **/
    public void ack(Tuple t, String url) {
        unacked.put(url, t);
    }

    @Override
	protected void store(String url, Status status, Metadata metadata, Date nextFetch) throws Exception {
		StringBuilder builder = new StringBuilder();
		builder.append(url).append("\t").append(status).append("\t").append(nextFetch.getTime()).append("\t")
				.append(metadata);
		TypedMessageBuilder<String> message = producer.newMessage().value(builder.toString());
		// set key for routing e.g host
		if (doRouting) {
			String key = partitioner.getPartition(url, metadata);
			message.key(key);
		}
		CompletableFuture<MessageId> messageID = message.sendAsync();

		// ack when we know it has successfully completed or fail otherwise
		messageID.handle((m, ex) -> {
			Tuple tuple = unacked.remove(url);
			if (tuple == null)
				return null;
			if (ex != null) {
				LOG.error("Exception with message {}", url, ex);
				_collector.fail(tuple);
			} else {
				super.ack(tuple, url);
			}
			return null;
		});

	}
}
